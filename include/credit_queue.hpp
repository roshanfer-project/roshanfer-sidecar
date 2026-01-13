#pragma once

#include "buffer.h"
#include "fast_map.hpp"
#include "rpc_message.h"
#include "spinlock.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string_view>

const size_t PriorityCount = 3;

/*
InnerCreditQueue is a credit queue for a single priority level that has a queue
for endpoint belonging to that priority level.
*/
class InnerCreditQueue {
public:
  InnerCreditQueue(std::vector<std::string>, int32_t, int32_t);

  // delete copy semantics
  InnerCreditQueue(const InnerCreditQueue &) = delete;
  InnerCreditQueue &operator=(const InnerCreditQueue &) = delete;

  // delete move semantics
  InnerCreditQueue(InnerCreditQueue &&) = delete;
  InnerCreditQueue &operator=(InnerCreditQueue &&) = delete;

  void push(std::unique_ptr<Buffer>, std::string_view);
  std::unique_ptr<Buffer> pop(int32_t &, LocalMap<int32_t> &);
  size_t size();

private:
  int ppm_limit;
  int per_endpoint_limit;
  LocalMap<std::deque<std::unique_ptr<Buffer>>> queue;
  LocalMap<std::deque<std::unique_ptr<Buffer>>>::iterator it;
  size_t _size;
};

class CreditQueue {
public:
  CreditQueue(std::vector<std::string>, int32_t, int32_t);

  // delete copy semantics
  CreditQueue(const CreditQueue &) = delete;
  CreditQueue &operator=(const CreditQueue &) = delete;

  // delete move semantics
  CreditQueue(CreditQueue &&) = delete;
  CreditQueue &operator=(CreditQueue &&) = delete;

  void push(std::unique_ptr<Buffer>, std::string_view, Priority);
  std::unique_ptr<Buffer> pop();
  size_t size();

  bool check_credit_available(std::string_view);
  void increment_in_flight(std::string_view);
  void decrement_in_flight(std::string_view);

private:
  std::array<InnerCreditQueue, PriorityCount> credit_queue;
  std::array<size_t, PriorityCount> weights;
  size_t it;
  size_t remaining_rounds;
  SpinLock lock;
  std::atomic<size_t> _size;

  // global limit counters
  int32_t in_flight;
  int32_t ppm_limit;

  // per-endpoint limit counters
  LocalMap<int32_t> in_flight_per_endpoint;
  int32_t per_endpoint_limit;
};