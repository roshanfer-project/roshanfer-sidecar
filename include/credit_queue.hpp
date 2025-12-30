#pragma once

#include "buffer.h"
#include "fast_map.hpp"
#include "spinlock.hpp"
#include <cstdint>
#include <deque>
#include <string_view>

class CreditQueue {
public:
  CreditQueue(std::vector<std::string>, int32_t, int32_t);

  // delete copy semantics
  CreditQueue(const CreditQueue &) = delete;
  CreditQueue &operator=(const CreditQueue &) = delete;

  // delete move semantics
  CreditQueue(CreditQueue &&) = delete;
  CreditQueue &operator=(CreditQueue &&) = delete;

  void push(std::unique_ptr<Buffer>, std::string_view);
  std::unique_ptr<Buffer> pop();
  size_t size();

  bool check_credit_available(std::string_view);
  void increment_in_flight(std::string_view);
  void decrement_in_flight(std::string_view);

private:
  LocalMap<std::deque<std::unique_ptr<Buffer>>> credit_queue;
  LocalMap<std::deque<std::unique_ptr<Buffer>>>::iterator it;
  SpinLock lock;
  std::atomic<size_t> _size;

  // global limit counters
  int32_t in_flight;
  int32_t ppm_limit;

  // per-endpoint limit counters
  LocalMap<int32_t> in_flight_per_endpoint;
  int32_t per_endpoint_limit;
};