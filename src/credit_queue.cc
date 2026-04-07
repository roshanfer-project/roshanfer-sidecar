#include "credit_queue.hpp"
#include "config.h"
#include "fast_map.hpp"
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

std::unique_ptr<Buffer> InnerCreditQueue::pop(
    int32_t &in_flight, LocalMap<int32_t> &in_flight_per_endpoint,
    int32_t &ppm_limit, LocalMap<int32_t> &per_endpoint_limit) {
  if (_size == 0) {
    return nullptr;
  }

  auto &init_endpoint = it->key;
  while (1) {
    if (it->value.size() > 0 &&
        in_flight_per_endpoint.get(it->key) < per_endpoint_limit.get(it->key) &&
        in_flight < ppm_limit) {
      auto buffer = std::move(it->value.front());

      // do not increment active requests for dfanout branches that are not the
      // actual rpc branch
      char b1 = buffer->data.at(1);
      if (b1 == 0x01) {
        in_flight_per_endpoint.get(it->key)++;
        in_flight++;
      } else if (b1 == 0x02) {
        // reset the b1 bit to normal DN
        buffer->data.at(1) = 0x01;
      } else {
        LOG(FATAL) << "Received DN with invalid b1" << (int)b1;
      }
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
      weights({16, 4, 1}), it(0), remaining_rounds(weights.at(it)), lock(),
      _size(0), in_flight(0), ppm_limit(0), in_flight_per_endpoint(endpoints),
      per_endpoint_limit(endpoints) {
  for (size_t i = 0; i < endpoints.size(); i++) {
    in_flight_per_endpoint.set(endpoints.at(i), 0);
    per_endpoint_limit.set(endpoints.at(i), cpu_count * 2 + config.extra_limit);
  }
  auto max_limit = cpu_count * 2 + config.extra_limit;
  auto sum_limit = max_limit * (int)endpoints.size();
  ppm_limit = max_limit + (int32_t)((float)(sum_limit - max_limit) *
                                    config.over_commitment.value_or(-1));
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
  lock.unlock();
  VLOG(1) << "CreditQueue: new endpoint limit. endpoint: " << api
          << ", limit: " << limit;
}

void CreditQueue::update_ppm_limit(int32_t limit) {
  lock.lock();
  ppm_limit = limit;
  lock.unlock();
  VLOG(1) << "CreditQueue: new global limit: " << limit;
}

std::unique_ptr<Buffer> CreditQueue::pop() {
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
    if (auto buffer = credit_queue.at(it).pop(in_flight, in_flight_per_endpoint,
                                              ppm_limit, per_endpoint_limit);
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

void CreditQueue::increment_in_flight(std::string_view api) {
  lock.lock();
  in_flight_per_endpoint.get(api)++;
  in_flight++;
  lock.unlock();
}

void CreditQueue::decrement_in_flight(std::string_view api) {
  lock.lock();
  in_flight_per_endpoint.get(api)--;
  in_flight--;
  lock.unlock();
}
