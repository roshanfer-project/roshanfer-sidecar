#include "credit_queue.hpp"
#include "fast_map.hpp"
#include "rpc_message.h"
#include <cstddef>
#include <cstdint>
#include <utility>

InnerCreditQueue::InnerCreditQueue(std::vector<std::string> endpoints,
                                   int32_t ppm_limit,
                                   int32_t per_endpoint_limit)
    : ppm_limit(ppm_limit), per_endpoint_limit(per_endpoint_limit),
      queue(endpoints), it(queue.begin()), _size(0) {}

void InnerCreditQueue::push(std::unique_ptr<Buffer> buffer,
                            std::string_view endpoint) {
  queue.get(endpoint).push_back(std::move(buffer));
  _size++;
}

std::unique_ptr<Buffer>
InnerCreditQueue::pop(int32_t &in_flight,
                      LocalMap<int32_t> &in_flight_per_endpoint) {
  if (_size == 0) {
    return nullptr;
  }

  auto &init_endpoint = it->key;
  while (1) {
    if (it->value.size() > 0 &&
        in_flight_per_endpoint.get(it->key) < per_endpoint_limit &&
        in_flight < ppm_limit) {
      in_flight_per_endpoint.get(it->key)++;
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

CreditQueue::CreditQueue(std::vector<std::string> endpoints, int32_t ppm_limit,
                         int32_t per_endpoint_limit)
    : credit_queue{{{endpoints, ppm_limit, per_endpoint_limit},
                    {endpoints, ppm_limit, per_endpoint_limit},
                    {endpoints, ppm_limit, per_endpoint_limit}}},
      weights({16, 4, 1}), it(0), remaining_rounds(weights.at(it)), lock(),
      _size(0), in_flight(0), ppm_limit(ppm_limit),
      in_flight_per_endpoint(endpoints),
      per_endpoint_limit(per_endpoint_limit) {
  for (size_t i = 0; i < endpoints.size(); i++) {
    in_flight_per_endpoint.set(endpoints.at(i), 0);
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
    if (auto buffer =
            credit_queue.at(it).pop(in_flight, in_flight_per_endpoint);
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

bool CreditQueue::check_credit_available(std::string_view api) {
  lock.lock();
  if (in_flight_per_endpoint.get(api) < per_endpoint_limit &&
      in_flight < ppm_limit) {
    in_flight_per_endpoint.get(api)++;
    in_flight++;
    lock.unlock();
    return true;
  }
  lock.unlock();
  return false;
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
