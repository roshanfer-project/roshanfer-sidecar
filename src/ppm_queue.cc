#include "ppm_queue.h"
#include "glog/logging.h"
#include "rpc_message.h"
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>

PPMQueue::PPMQueue()
: ppm_queue(std::unordered_map<std::string, std::queue<std::shared_ptr<RPCMessage>>,
    TransparentHash, TransparentEqual>()) {}

void PPMQueue::enqueue(std::shared_ptr<RPCMessage> rpc) {
    if (ppm_queue.find(rpc->service) == ppm_queue.end()) {
        ppm_queue.emplace(rpc->service, std::queue<std::shared_ptr<RPCMessage>>());
    }
    ppm_queue[rpc->service].push(rpc);
    VLOG(1) << "Enqueued RPC message on service " << rpc->service;
}

std::shared_ptr<RPCMessage> PPMQueue::dequeue(const std::string& service) {
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
    return it->second.front()->us_fd;
}