#include "ppm_queue.h"
#include "config.h"
#include "glog/logging.h"
#include "rpc_message.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>

PPMQueue::PPMQueue(std::unordered_map<std::string, RoutingEntry,
                                      TransparentHash, TransparentEqual>
                       routing) {
  for (const auto &[route, _] : routing) {
    ppm_queue.emplace(route, std::deque<std::shared_ptr<RPCMessage>>());
  }
}

void PPMQueue::push(std::shared_ptr<RPCMessage> rpc) {
  try {
    ppm_queue.at(rpc->get_service()).push_back(rpc);
  } catch (const std::out_of_range &e) {
    LOG(FATAL) << "Error in pushing RPC message: " << e.what()
               << " service: " << rpc->get_service();
  } catch (const std::exception &e) {
    LOG(FATAL) << "Error in pushing RPC message: " << e.what()
               << " service: " << rpc->get_service();
  }
  VLOG(1) << "PPMQueue: Pushed RPC message "
          << "| service: " << rpc->get_service()
          << "| id: " << rpc->get_id_string();
}

std::shared_ptr<RPCMessage> PPMQueue::pop(const std::string &service,
                                          RPCID id) {
  try {
    if (ppm_queue.at(service).empty()) {
      LOG(FATAL) << "Trying to pop from an empty queue for service: "
                 << service;
    }
    auto &queue = ppm_queue.at(service);
    auto it = queue.begin();
    while (it != queue.end()) {
      if ((*it)->get_local_id() == id) {
        auto rpc = *it;
        queue.erase(it);
        VLOG(1) << "PPMQueue: Popped RPC message "
                << "| service: " << service << "| id: " << id
                << "| ppm_queue size: " << queue.size();
        return rpc;
      }
      it++;
    }
    LOG(FATAL) << "Trying to pop from a queue with an invalid id. "
               << "| id: " << id << "| service: " << service;

  } catch (const std::out_of_range &) {
    LOG(FATAL) << "Service not found in PPM queue: " << service;
  } catch (const std::exception &e) {
    LOG(FATAL) << "Error in dequeueing RPC message: " << e.what()
               << " service: " << service;
  }
}

std::shared_ptr<RPCMessage> PPMQueue::pop(const std::string &service) {
  try {
    auto &queue = ppm_queue.at(service);
    if (queue.empty()) {
      LOG(FATAL) << "Trying to pop from an empty queue for service: "
                 << service;
    }
    auto rpc = queue.front();
    queue.pop_front();
    VLOG(1) << "PPMQueue: Popped RPC message "
            << "| service: " << service << "| id: " << rpc->get_local_id()
            << "| ppm_queue size: " << queue.size();
    return rpc;

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

int32_t PPMQueue::get_waiting_delay_us(const std::string &service) {
  try {
    if (ppm_queue.at(service).empty()) {
      return 0;
    }
    auto first_req_for = ppm_queue.at(service).front()->req_rcv_time;
    return (int32_t)std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now() - first_req_for)
        .count();
  } catch (const std::out_of_range &e) {
    LOG(FATAL) << "Service not found in PPM queue: " << service;
  } catch (const std::exception &e) {
    LOG(FATAL) << "Error in getting queueing delay from PPM queue: " << e.what()
               << " service: " << service;
  }
}
