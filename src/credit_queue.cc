#include "credit_queue.hpp"
#include "config.h"
#include "fast_map.hpp"
#include "ppm_queue.h"
#include "rpc_message.h"
#include <cstddef>
#include <cstdint>
#include <utility>

InnerCreditQueue::InnerCreditQueue(std::vector<std::string> endpoints)
    : queue(endpoints), it(queue.begin()), _size(0) {}

void InnerCreditQueue::push(std::unique_ptr<Buffer> buffer,
                            std::string_view endpoint) {
  queue.get(endpoint).push_back(std::move(buffer));
  _size++;
}

static int sum_ds_waiting(PPMQueue &ppm_queue, std::string &us) {
  auto sum = 0;
  for (auto &ds : config.mapping.at(us).downstreams) {
    sum += ppm_queue.size(ds);
  }
  return sum;
}

std::unique_ptr<Buffer> InnerCreditQueue::pop(int32_t &in_flight,
                                              int32_t &ppm_limit,
                                              PPMQueue &ppm_queue) {
  if (_size == 0) {
    return nullptr;
  }

  auto &init_endpoint = it->key;
  while (1) {
    if (it->value.size() > 0 && in_flight < ppm_limit &&
        sum_ds_waiting(ppm_queue, it->key) == 1) {
      in_flight++;
      auto buffer = std::move(it->value.front());
      it->value.pop_front();
      it++;
      _size--;
      return buffer;
    }

    it++;
    if (it->key == init_endpoint) {
      break;
    }
  }

  return nullptr;
}

CreditQueue::CreditQueue(std::vector<std::string> endpoints, int32_t cpu_count)
    : credit_queue{{{endpoints}, {endpoints}, {endpoints}}},
      weights({16, 4, 1}), endpoints(endpoints), it(0),
      remaining_rounds(weights.at(it)), lock(), _size(0), in_flight(0),
      ppm_limit(0), per_endpoint_limit(endpoints) {
  auto max_limit = cpu_count * 2 + config.extra_limit;
  ppm_limit = max_limit;
  for (auto &endpoint : endpoints) {
    per_endpoint_limit.set(endpoint, max_limit);
  }
}

size_t CreditQueue::size() { return _size.load(); }

void CreditQueue::push(std::unique_ptr<Buffer> buffer,
                       std::string_view endpoint, Priority priority) {
  lock.lock();
  credit_queue.at((size_t)priority).push(std::move(buffer), endpoint);
  lock.unlock();
  _size.fetch_add(1);
}

void CreditQueue::update_endpoint_limit(int32_t limit, std::string_view api) {
  lock.lock();
  per_endpoint_limit.set(api, limit);
  int32_t max = 0;
  for (auto &endpoint : endpoints) {
    auto plimit = per_endpoint_limit.get(endpoint);
    if (plimit > max) {
      max = plimit;
    }
  }
  ppm_limit = max;
  lock.unlock();
  VLOG(1) << "CreditQueue: new endpoint limit. endpoint: " << api
          << ", limit: " << limit;
}

std::unique_ptr<Buffer> CreditQueue::pop(PPMQueue &ppm_queue) {
  if (_size.load() == 0) {
    return nullptr;
  }

  lock.lock();

  // global limit check
  if (in_flight >= ppm_limit) {
    lock.unlock();
    return nullptr;
  }

  size_t init_index = it;
  while (1) {
    if (auto buffer = credit_queue.at(it).pop(in_flight, ppm_limit, ppm_queue);
        buffer != nullptr) {
      remaining_rounds--;
      if (remaining_rounds == 0) {
        it = (it + 1) % PriorityCount;
        remaining_rounds = weights.at(it);
      }
      _size.fetch_sub(1);
      lock.unlock();
      return buffer;
    }

    // advance iterator
    it = (it + 1) % PriorityCount;
    remaining_rounds = weights.at(it);
    // check for wrap
    if (it == init_index) {
      break;
    }
  }

  lock.unlock();
  return nullptr;
}

void CreditQueue::increment_in_flight() {
  lock.lock();
  in_flight++;
  lock.unlock();
}

void CreditQueue::decrement_in_flight() {
  lock.lock();
  in_flight--;
  lock.unlock();
}
