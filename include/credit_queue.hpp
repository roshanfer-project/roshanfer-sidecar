#pragma once

#include "buffer.h"
#include "fast_map.hpp"
#include "ppm_queue.h"
#include "rpc_message.h"
#include "spinlock.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <string_view>
#include <vector>

const size_t PriorityCount = 3;

/*
InnerCreditQueue is a credit queue for a single priority level that has a queue
for endpoint belonging to that priority level.
*/
class InnerCreditQueue {
public:
  InnerCreditQueue(std::vector<std::string>);

  // delete copy semantics
  InnerCreditQueue(const InnerCreditQueue &) = delete;
  InnerCreditQueue &operator=(const InnerCreditQueue &) = delete;

  // delete move semantics
  InnerCreditQueue(InnerCreditQueue &&) = delete;
  InnerCreditQueue &operator=(InnerCreditQueue &&) = delete;

  void push(std::unique_ptr<Buffer>, std::string_view);
  std::unique_ptr<Buffer> pop(int32_t &, int32_t &, PPMQueue &);
  size_t size();

private:
  LocalMap<std::deque<std::unique_ptr<Buffer>>> queue;
  LocalMap<std::deque<std::unique_ptr<Buffer>>>::iterator it;
  size_t _size;
};

class CreditQueue {
public:
  CreditQueue(std::vector<std::string>, int32_t);

  // delete copy semantics
  CreditQueue(const CreditQueue &) = delete;
  CreditQueue &operator=(const CreditQueue &) = delete;

  // delete move semantics
  CreditQueue(CreditQueue &&) = delete;
  CreditQueue &operator=(CreditQueue &&) = delete;

  void push(std::unique_ptr<Buffer>, std::string_view, Priority);
  std::unique_ptr<Buffer> pop(PPMQueue &);
  size_t size();

  void increment_in_flight();
  void decrement_in_flight();
  void update_endpoint_limit(int32_t, std::string_view);

  int32_t get_ppm_limit() { return ppm_limit; }

private:
  std::array<InnerCreditQueue, PriorityCount> credit_queue;
  std::array<size_t, PriorityCount> weights;
  std::vector<std::string> endpoints;
  size_t it;
  size_t remaining_rounds;
  SpinLock lock;
  std::atomic<size_t> _size;

  // global limit counters
  int32_t in_flight;
  int32_t ppm_limit;

  // per-endpoint limit counters (used for finding maximum)
  LocalMap<int32_t> per_endpoint_limit;
};