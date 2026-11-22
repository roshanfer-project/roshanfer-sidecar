// Cross-process signalling delay of an SPSC ring with an io_uring futex
// doorbell (docs/plan/futex-ring.md). The parent pushes records at a fixed
// rate; the child sleeps in IORING_OP_FUTEX_WAIT on `tail` and records
// push-to-seen delay. Run in a Release build, optionally pinned:
//   ./build/futex_ring_bench [prod_cpu cons_cpu]
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <glog/logging.h>
#include <liburing.h>
#include <linux/futex.h>
#include <new>
#include <sched.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {

constexpr uint32_t kSlots = 4096; // power of two
constexpr uint64_t kStop = UINT64_MAX;
constexpr int kSeconds = 2;

struct alignas(64) Slot {
  uint64_t seq;
  uint64_t t_push_ns;
};

struct Region {
  alignas(64) std::atomic<uint32_t> tail;
  alignas(64) std::atomic<uint32_t> head;
  alignas(64) std::atomic<uint32_t> ready;
  Slot slots[kSlots];
};
static_assert(std::atomic<uint32_t>::is_always_lock_free);
static_assert(sizeof(std::atomic<uint32_t>) == sizeof(uint32_t));
static_assert(sizeof(Slot) == 64);

uint64_t now_ns() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return uint64_t(ts.tv_sec) * 1'000'000'000ULL + uint64_t(ts.tv_nsec);
}

void pin(int cpu) {
  if (cpu < 0)
    return;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  PCHECK(sched_setaffinity(0, sizeof(set), &set) == 0) << "pin to " << cpu;
}

// Shared (no FUTEX2_PRIVATE): both processes key the futex on the memfd page.
void prep_wait(io_uring_sqe *sqe, std::atomic<uint32_t> *word,
               uint32_t expected) {
  io_uring_prep_futex_wait(sqe, reinterpret_cast<uint32_t *>(word), expected,
                           FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
}

void prep_wake(io_uring_sqe *sqe, std::atomic<uint32_t> *word) {
  io_uring_prep_futex_wake(sqe, reinterpret_cast<uint32_t *>(word), 1,
                           FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
}

io_uring_sqe *get_sqe(io_uring *ring) {
  io_uring_sqe *sqe = io_uring_get_sqe(ring);
  CHECK(sqe != nullptr) << "SQ full";
  return sqe;
}

void consumer(Region *r, size_t max_samples, int cpu) {
  pin(cpu);
  alarm(kSeconds + 10); // a lost wake hangs here; SIGALRM kills the child
  io_uring ring;
  CHECK_EQ(io_uring_queue_init(8, &ring, 0), 0);
  std::vector<uint64_t> delays;
  delays.reserve(max_samples);
  uint64_t slept = 0, eagain = 0, empty = 0;
  uint32_t head = 0;

  prep_wait(get_sqe(&ring), &r->tail, head);
  CHECK_EQ(io_uring_submit(&ring), 1);
  r->ready.store(1, std::memory_order_release);

  for (bool stop = false; !stop;) {
    io_uring_cqe *cqe;
    CHECK_EQ(io_uring_wait_cqe(&ring, &cqe), 0);
    int res = cqe->res;
    io_uring_cqe_seen(&ring, cqe);
    if (res == 0)
      slept++;
    else if (res == -EAGAIN)
      eagain++;
    else
      LOG(FATAL) << "FUTEX_WAIT cqe res " << res;

    uint32_t tail = r->tail.load(std::memory_order_acquire);
    uint64_t seen = now_ns();
    if (tail == head)
      empty++; // a late wake for records already drained
    for (; head != tail; head++) {
      const Slot &s = r->slots[head & (kSlots - 1)];
      if (s.seq == kStop) {
        stop = true;
        continue;
      }
      CHECK_LT(delays.size(), max_samples) << "delay buffer full";
      delays.push_back(seen - s.t_push_ns);
    }
    r->head.store(head, std::memory_order_release);
    if (!stop) {
      prep_wait(get_sqe(&ring), &r->tail, head);
      CHECK_EQ(io_uring_submit(&ring), 1);
    }
  }
  io_uring_queue_exit(&ring);

  size_t n = delays.size();
  CHECK_GT(n, size_t{10});
  std::sort(delays.begin() + long(n / 10), delays.end()); // drop 10% warmup
  auto pct = [&](double p) {
    size_t lo = n / 10;
    size_t i = lo + size_t(p * double(n - lo - 1));
    return double(delays[i]) / 1000.0;
  };
  std::printf("  p50 %8.2f  p99 %8.2f  p99.9 %8.2f  max %9.2f us"
              "  | records %8zu  slept %8lu  eagain %8lu  empty %8lu\n",
              pct(0.50), pct(0.99), pct(0.999), pct(1.0), n, slept, eagain,
              empty);
}

void push(Region *r, io_uring *ring, uint32_t &tail, uint64_t seq) {
  CHECK_LT(tail - r->head.load(std::memory_order_acquire), kSlots)
      << "ring full";
  r->slots[tail & (kSlots - 1)] = {seq, now_ns()};
  tail++;
  r->tail.store(tail, std::memory_order_release);
  // One wake per push; successful wakes post no CQE.
  io_uring_sqe *sqe = get_sqe(ring);
  prep_wake(sqe, &r->tail);
  sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
  CHECK_EQ(io_uring_submit(ring), 1);
  io_uring_cqe *cqe;
  if (io_uring_peek_cqe(ring, &cqe) == 0)
    LOG(FATAL) << "FUTEX_WAKE cqe res " << cqe->res;
}

void run(uint64_t rate, int prod_cpu, int cons_cpu) {
  int fd = memfd_create("futex_ring_bench", 0);
  PCHECK(fd >= 0) << "memfd_create";
  PCHECK(ftruncate(fd, sizeof(Region)) == 0) << "ftruncate";
  void *p = mmap(nullptr, sizeof(Region), PROT_READ | PROT_WRITE, MAP_SHARED,
                 fd, 0);
  PCHECK(p != MAP_FAILED) << "mmap";
  auto *r = new (p) Region{}; // fresh zero pages; construct the atomics

  uint64_t total = rate * kSeconds;
  pid_t pid = fork();
  PCHECK(pid >= 0) << "fork";
  if (pid == 0) {
    consumer(r, total + 1, cons_cpu);
    std::fflush(stdout); // _exit skips stdio flush
    _exit(0);
  }

  pin(prod_cpu);
  io_uring ring;
  CHECK_EQ(io_uring_queue_init(8, &ring, 0), 0);
  while (r->ready.load(std::memory_order_acquire) == 0) {
  }

  uint32_t tail = 0;
  uint64_t period = 1'000'000'000ULL / rate;
  uint64_t start = now_ns(), next = start;
  for (uint64_t seq = 0; seq < total; seq++) {
    while (now_ns() < next) {
    }
    push(r, &ring, tail, seq);
    next += period;
  }
  double secs = double(now_ns() - start) / 1e9;
  // Print before the stop record, so the child's line follows on stdout.
  std::printf("rate %8lu/s  achieved %9.0f/s", rate, double(total) / secs);
  std::fflush(stdout);
  push(r, &ring, tail, kStop);

  int status;
  PCHECK(waitpid(pid, &status, 0) == pid) << "waitpid";
  CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0)
      << "consumer died, status " << status;
  io_uring_queue_exit(&ring);
  munmap(p, sizeof(Region));
  close(fd);
}

} // namespace

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  int prod_cpu = -1, cons_cpu = -1;
  if (argc == 3) {
    prod_cpu = std::atoi(argv[1]);
    cons_cpu = std::atoi(argv[2]);
  } else if (argc != 1) {
    std::fprintf(stderr, "usage: %s [prod_cpu cons_cpu]\n", argv[0]);
    return 1;
  }
  for (uint64_t rate : {1'000ULL, 10'000ULL, 100'000ULL, 1'000'000ULL})
    run(rate, prod_cpu, cons_cpu);
  return 0;
}
