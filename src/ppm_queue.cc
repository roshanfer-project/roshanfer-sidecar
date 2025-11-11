#include "ppm_queue.h"
#include "config.h"
#include "glog/logging.h"
#include "rpc_message.h"
#include <string>
#include <unordered_map>

PPMQueue::PPMQueue(std::unordered_map<std::string, RoutingEntry, TransparentHash, TransparentEqual> routing)
: ppm_queue(std::unordered_map<std::string, std::unordered_map<int16_t, std::shared_ptr<RPCMessage>>,
    TransparentHash, TransparentEqual>()) {
    for (const auto& [route, _] : routing) {
        ppm_queue.emplace(route, std::unordered_map<int16_t, std::shared_ptr<RPCMessage>>());
    }
}

void PPMQueue::push(std::shared_ptr<RPCMessage> rpc) {
    try {
        ppm_queue.at(rpc->get_service()).emplace(rpc->get_id(), rpc);
    } catch (const std::out_of_range& e) {
        LOG(FATAL) << "Error in pushing RPC message: " << e.what()
                    << " service: " << rpc->get_service();
    } catch (const std::exception& e) {
        LOG(FATAL) << "Error in pushing RPC message: " << e.what()
                    << " service: " << rpc->get_service();
    }
    VLOG(1) << "PPMQueue: Pushed RPC message "
            << "| service: " << rpc->get_service()
            << "| id: " << rpc->get_id();
}

std::shared_ptr<RPCMessage> PPMQueue::pop(const std::string& service, int16_t id) {
    try {
        if (ppm_queue.at(service).empty()) {
            LOG(FATAL) << "Trying to pop from an empty queue for service: " << service;
        }
        auto rpc = ppm_queue.at(service).at(id);
        ppm_queue.at(service).erase(id);
        VLOG(1) << "PPMQueue: Popped RPC message "
                << "| service: " << service
                << "| id: " << id
                << "| ppm_queue size: " << ppm_queue.at(service).size();
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
        LOG(FATAL) << "Trying to find from an empty queue"
                   << " service: " << service;
    }
    return it->first;
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