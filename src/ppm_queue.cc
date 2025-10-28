#include "ppm_queue.h"
#include "config.h"
#include "glog/logging.h"
#include "rpc_message.h"
#include <chrono>
#include <queue>
#include <string>
#include <unordered_map>

PPMQueue::PPMQueue(std::unordered_map<std::string, RoutingEntry, TransparentHash, TransparentEqual> routing)
: ppm_queue(std::unordered_map<std::string, std::queue<std::shared_ptr<RPCMessage>>,
    TransparentHash, TransparentEqual>()) {
    for (const auto& [route, _] : routing) {
        ppm_queue.emplace(route, std::queue<std::shared_ptr<RPCMessage>>());
    }
}

void PPMQueue::enqueue(std::shared_ptr<RPCMessage> rpc) {
    try {
        ppm_queue.at(rpc->get_service()).push(rpc);
    } catch (const std::out_of_range&) {
        try {
            ppm_queue.emplace(rpc->get_service(), std::queue<std::shared_ptr<RPCMessage>>());
            ppm_queue.at(rpc->get_service()).push(rpc);
        } catch (const std::exception& e) {
            LOG(FATAL) << "Error in initializing PPM queue for service: "
                       << rpc->get_service() << ", error: " << e.what();
        }
    } catch (const std::exception& e) {
        LOG(FATAL) << "Error in enqueueing RPC message: " << e.what()
                   << " service: " << rpc->get_service();
    }
    VLOG(1) << "Enqueued RPC message on service " << rpc->get_service();
}

std::shared_ptr<RPCMessage> PPMQueue::dequeue(const std::string& service) {
    try {
        if (ppm_queue.at(service).empty()) {
            LOG(FATAL) << "Trying to dequeue from an empty queue for service: " << service;
        }
        auto rpc = ppm_queue.at(service).front();
        ppm_queue.at(service).pop();
        VLOG(1) << "Dequeued RPC message on service " << service;
        return rpc;
    } catch (const std::out_of_range&) {
        LOG(FATAL) << "Service not found in PPM queue: " << service;
    } catch (const std::exception& e) {
        LOG(FATAL) << "Error in dequeueing RPC message: " << e.what()
                   << " service: " << service;
    }
}

const std::string& PPMQueue::check(std::string_view& service) {
    auto it = ppm_queue.find(service);
    if (it == ppm_queue.end()) {
        LOG(INFO) << "ppm_queue size: " << ppm_queue.size()
                    << " keys: ";
        for (const auto& kv : ppm_queue) {
            LOG(INFO) << "'" << kv.first << "'";
        }
        LOG(FATAL) << "Service not found in PPM queue: " << service << " (size: " << service.length() << ")";
    }
    if (it->second.empty()) {
        LOG(FATAL) << "Trying to find from an empty queue";
    }
    return it->first;
}

int PPMQueue::get_fd(const std::string& service) {
    if (ppm_queue.find(service) == ppm_queue.end()) {
        LOG(FATAL) << "Service not found in PPM queue: " << service;
    }
    if (ppm_queue.at(service).empty()) {
        LOG(FATAL) << "Trying to get fd from an empty queue for service: " << service;
    }
    return ppm_queue.at(service).front()->get_us_fd();
}

size_t PPMQueue::size(const std::string& service) {
    try {
        return ppm_queue.at(service).size();
    } catch (const std::out_of_range& e) {
        LOG(FATAL) << "Service not found in PPM queue: " << service;
    } catch (const std::exception& e) {
        LOG(FATAL) << "Error in getting size from PPM queue: " << e.what()
                   << " service: " << service;
    }
}

int PPMQueue::queueing_delay(const std::string& service) {
    try {
        auto it = ppm_queue.find(service);
        if (it == ppm_queue.end()) {
            LOG(FATAL) << "Service not found in PPM queue: " << service;
        }
        if (it->second.empty()) {
            return 0;
        }
        return (int)std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()
                            - it->second.front()->req_rcv_time).count();
    } catch (const std::out_of_range& e) {
        LOG(FATAL) << "Trying to get queueing delay from an empty queue for service: " << service;
    } catch (const std::exception& e) {
        LOG(FATAL) << "Error in getting queueing delay from PPM queue: " << e.what()
                   << " service: " << service;
    }
}