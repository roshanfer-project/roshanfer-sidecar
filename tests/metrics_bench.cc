// Per-record cost of the metrics library. Run pinned in a Release build:
//   taskset -c 2 ./build/metrics_bench
#include "metrics.h"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

namespace {

constexpr size_t kOps = 1'000'000;
constexpr int kRuns = 5;

// Median ns/op over kRuns; `setup` runs untimed before each run. Cases 2 and 4
// use kCapacity (64 MB): above glibc's mmap threshold, so no-prefault pages
// are really fresh.
template <class Setup, class Body> double bench(Setup setup, Body body) {
  std::vector<double> ns;
  for (int r = 0; r < kRuns; r++) {
    setup();
    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kOps; i++)
      body(i);
    auto t1 = std::chrono::steady_clock::now();
    ns.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count() /
                 kOps);
  }
  std::sort(ns.begin(), ns.end());
  return ns[kRuns / 2];
}

volatile int64_t sink_ts; // keeps the clock-only loop from being elided

} // namespace

int main() {
  const std::array<std::string, 2> methods = {
      "/hotel.Search/Nearby", "/hotel.Reservation/CheckAvailability"};
  uint16_t id = 0, e2e = 0, req_for = 0, res_for = 0;
  metrics::MethodCache cache;

  auto fresh = [&](size_t capacity, bool prefault) {
    metrics::init("bench", capacity, prefault);
    id = metrics::id("EMA DS-RT-svc T:T");
    e2e = metrics::id("E2E INGRESS svc");
    req_for = metrics::id("REQ-FOR INGRESS svc");
    res_for = metrics::id("RES-FOR INGRESS svc");
    cache = metrics::MethodCache{};
  };
  auto rpc = [&](const std::string &m, size_t i) {
    const uint16_t mid = cache.get(m);
    const int64_t ts = metrics::now_us();
    metrics::record(e2e, static_cast<float>(i), mid, ts);
    metrics::record(req_for, static_cast<float>(i), mid, ts);
    metrics::record(res_for, static_cast<float>(i), mid, ts);
  };

  double clock = bench([] {}, [](size_t) { sink_ts = metrics::now_us(); });
  double rec = bench([&] { fresh(metrics::kCapacity, true); },
                     [&](size_t i) { metrics::record(id, (float)i); });
  double hit = bench([&] { fresh(3 * kOps, true); },
                     [&](size_t i) { rpc(methods[0], i); });
  double miss = bench([&] { fresh(3 * kOps, true); },
                      [&](size_t i) { rpc(methods[i & 1], i); });
  double cold = bench([&] { fresh(metrics::kCapacity, false); },
                      [&](size_t i) { metrics::record(id, (float)i); });

  std::printf("ns/op (median of %d x %zu ops)\n", kRuns, kOps);
  std::printf("  1  clock_gettime(REALTIME)          %6.1f\n", clock);
  std::printf("  2  record(id, v)                    %6.1f\n", rec);
  std::printf("  3a one RPC, method cache hit        %6.1f\n", hit);
  std::printf("  3b one RPC, method cache miss       %6.1f\n", miss);
  std::printf("  4  record(id, v), no prefault       %6.1f\n", cold);
  return 0;
}
