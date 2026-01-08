#include "credit_queue.hpp"
#include <cstddef>

CreditQueue::CreditQueue(std::vector<std::string> endpoints, int32_t ppm_limit,
                         int32_t per_endpoint_limit)
    : credit_queue(endpoints), it(credit_queue.begin()), lock(), _size(0),
      in_flight(0), ppm_limit(ppm_limit), in_flight_per_endpoint(endpoints),
      per_endpoint_limit(per_endpoint_limit) {
  for (size_t i = 0; i < endpoints.size(); i++) {
    in_flight_per_endpoint.set(endpoints.at(i), 0);
  }
  it = credit_queue.begin();
}

size_t CreditQueue::size() { return _size.load(); }

void CreditQueue::push(std::unique_ptr<Buffer> buffer, std::string_view api) {
  lock.lock();
  credit_queue.get(api).push_back(std::move(buffer));
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

  std::string_view init_api = it->key;
  while (1) {
    if (it->value.size() != 0 &&
        in_flight_per_endpoint.get(it->key) < per_endpoint_limit &&
        in_flight < ppm_limit) {
      in_flight_per_endpoint.get(it->key)++;
      in_flight++;
      auto buffer = std::move(it->value.front());
      it->value.pop_front();
      it++;
      _size.fetch_sub(1);
      lock.unlock();
      return buffer;
    }

    // advance iterator
    it++;
    // check for wrap
    if (it->key == init_api) {
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
