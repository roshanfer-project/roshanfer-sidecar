#include "ppm_queue.h"
#include "glog/logging.h"
#include "rpc_message.h"
#include <queue>
#include <string>
#include <unordered_map>

PPMQueue::PPMQueue()
: ppm_queue(std::unordered_map<std::string, std::queue<RPCMessage*>,
    TransparentHash, TransparentEqual>()) {}

void PPMQueue::enqueue(RPCMessage* rpc) {
    if (ppm_queue.find(rpc->get_service()) == ppm_queue.end()) {
        ppm_queue.emplace(rpc->get_service(), std::queue<RPCMessage*>());
    }
    ppm_queue[rpc->get_service()].push(rpc);
    VLOG(1) << "Enqueued RPC message on service " << rpc->get_service();
}

RPCMessage* PPMQueue::dequeue(const std::string& service) {
    if (ppm_queue.find(service) == ppm_queue.end()) {
        LOG(FATAL) << "Service not found in PPM queue: " << service;
    }
    if (ppm_queue[service].empty()) {
        LOG(FATAL) << "Trying to dequeue from an empty queue";
    }
    auto rpc = ppm_queue[service].front();
    ppm_queue[service].pop();
    VLOG(1) << "Dequeued RPC message on service " << service;
    return rpc;
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
    auto it = ppm_queue.find(service);
    if (it == ppm_queue.end()) {
        LOG(FATAL) << "Service not found in PPM queue: " << service;
    }
    if (it->second.empty()) {
        LOG(FATAL) << "Trying to find from an empty queue";
    }
    return it->second.front()->get_us_fd();
}