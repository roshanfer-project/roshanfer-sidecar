#include "ppm_queue.h"
#include "config.h"
#include "glog/logging.h"
#include "utils.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>

PPMQueue::PPMQueue(std::unordered_map<std::string, RoutingEntry,
                                      TransparentHash, TransparentEqual>
                       routing) {
  for (const auto &[route, _] : routing) {
    ppm_queue.emplace(route, FlexiblePriorityQueue());
  }
}

void PPMQueue::push(const std::string &service, RPCID id, int64_t priority) {
  try {
    ppm_queue.at(service).push(id, priority);
  } catch (const std::out_of_range &e) {
    LOG(FATAL) << "Error in pushing RPC message: " << e.what()
               << " service: " << service;
  } catch (const std::exception &e) {
    LOG(FATAL) << "Error in pushing RPC message: " << e.what()
               << " service: " << service;
  }
  VLOG(1) << "PPMQueue: Pushed RPC message "
          << "| service: " << service << "| id: " << id;
}

RPCID PPMQueue::pop(const std::string &service, RPCID id) {
  try {
    auto &queue = ppm_queue.at(service);
    VLOG(1) << "PPMQueue: Popped RPC message "
            << "| service: " << service << "| id: " << id
            << "| ppm_queue size: " << queue.size();
    return queue.pop(id);

  } catch (const std::out_of_range &) {
    LOG(FATAL) << "Service not found in PPM queue: " << service;
  } catch (const std::exception &e) {
    LOG(FATAL) << "Error in dequeueing RPC message: " << e.what()
               << " service: " << service;
  }
}

RPCID PPMQueue::pop(const std::string &service) {
  try {
    auto &queue = ppm_queue.at(service);
    auto id = queue.pop();
    VLOG(1) << "PPMQueue: Popped RPC message "
            << "| service: " << service << "| id: " << id
            << "| ppm_queue size: " << queue.size();
    return id;

  } catch (const std::out_of_range &) {
    LOG(FATAL) << "Service not found in PPM queue: " << service;
  } catch (const std::exception &e) {
    LOG(FATAL) << "Error in dequeueing RPC message: " << e.what()
               << " service: " << service;
  }
}

const std::string &PPMQueue::check(std::string_view &service) {
  auto it = ppm_queue.find(service);
  if (it == ppm_queue.end()) {
    LOG(INFO) << "ppm_queue size: " << ppm_queue.size() << " keys: ";
    for (const auto &kv : ppm_queue) {
      LOG(INFO) << "'" << kv.first << "'";
    }
    LOG(FATAL) << "Service not found in PPM queue: " << service
               << " (size: " << service.length() << ")";
  }
  return it->first;
}

size_t PPMQueue::size(const std::string &service) {
  try {
    return ppm_queue.at(service).size();
  } catch (const std::out_of_range &e) {
    LOG(FATAL) << "Service not found in PPM queue: " << service;
  } catch (const std::exception &e) {
    LOG(FATAL) << "Error in getting size from PPM queue: " << e.what()
               << " service: " << service;
  }
}
