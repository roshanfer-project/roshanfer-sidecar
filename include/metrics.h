#pragma once

// Per-thread in-memory metrics: a preallocated array of raw samples, served
// as text on /dump (port kDumpBasePort + thread index). Only the owning thread
// touches its instance, so nothing is synchronized. Call sites compile only
// under SIDECAR_METRICS (--metrics).

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <string>
#include <string_view>

namespace metrics {

constexpr size_t kCapacity = size_t{4} << 20; // 4M records = 64 MB per thread
constexpr uint16_t kDumpBasePort = 5000;

struct Record {
  int64_t ts_us;
  uint16_t id;
  uint16_t method; // 0 = none
  float value;
};
static_assert(sizeof(Record) == 16);

// Trivially constructible so thread_local access compiles to a plain TLS
// load, with no init guard. `data` is a non-owning view of the array that
// metrics.cc owns.
struct Sink {
  Record *data;
  size_t size;
  size_t capacity;
};
extern thread_local Sink sink;

// (Re)initializes this thread's instance. `label` heads the dump.
void init(std::string label, size_t capacity = kCapacity,
          bool prefault = true);

// Intern a metric name / RPC method. Ids are per thread; id 0 means unset.
uint16_t id(std::string_view name);
uint16_t method(std::string_view m);

[[noreturn]] void fatal_full(uint16_t id);

inline int64_t now_us() {
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return static_cast<int64_t>(ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
}

inline void record(uint16_t id, float v, uint16_t method = 0,
                   int64_t ts = now_us()) {
  if (sink.size == sink.capacity) [[unlikely]]
    fatal_full(id);
  sink.data[sink.size++] = Record{ts, id, method, v};
}

// Remembers the last method seen, so a hit costs a length check + memcmp.
struct MethodCache {
  std::string last;
  uint16_t mid = 0;

  uint16_t get(std::string_view m) {
    if (mid != 0 && m == last)
      return mid;
    mid = method(m);
    last.assign(m);
    return mid;
  }
};

// Appends this thread's dump as text:
//   # <label>
//   # name <id> <name>
//   # method <id> <method>
//   <ts_us> <id> <method> <value>
void dump(std::string &out);

// Blocking TCP listen socket on 127.0.0.1:port.
int listen(uint16_t port);

// Reads the request, writes an HTTP/1.0 200 response holding dump(), closes.
void serve(int client_fd);

} // namespace metrics
