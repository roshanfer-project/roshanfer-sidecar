#include "ingress.h"
#include "connection_enums.h"
#include "fast_map.hpp"
#include "glog/logging.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

LocalMap<int32_t> make_drop_id(const std::unordered_map<std::string, RoutingEntry, TransparentHash, TransparentEqual>& routing) {
    std::vector<std::string> services;
    for (const auto& [route, info] : routing) {
        services.push_back(route);
    }
    return LocalMap<int32_t>(services);
}

LocalMap<std::chrono::time_point<std::chrono::steady_clock>> make_last_admission(const std::unordered_map<std::string,
     RoutingEntry, TransparentHash, TransparentEqual>& routing) {
    std::vector<std::string> services;
    for (const auto& [route, info] : routing) {
        services.push_back(route);
    }
    return LocalMap<std::chrono::time_point<std::chrono::steady_clock>>(services);
}

LocalMap<MovingAverage> make_admission_rate(const std::unordered_map<std::string,
     RoutingEntry, TransparentHash, TransparentEqual>& routing) {
    std::vector<std::string> services;
    for (const auto& [route, info] : routing) {
        services.push_back(route);
    }
    return LocalMap<MovingAverage>(services);
}

Ingress::Ingress(std::unordered_map<std::string, RoutingEntry, TransparentHash, TransparentEqual> routing, int index_arg) 
    :   queue(std::unordered_map<std::string, std::deque<std::shared_ptr<RPCMessage>>>()),
        drop_id(make_drop_id(routing)),
        services(std::vector<std::string>(routing.size())),
        drop_fd(-index_arg),
        p95(make_drop_id(routing)),
        p50(make_drop_id(routing)),
        slo(make_drop_id(routing)),
        admission_rate(make_admission_rate(routing)),
        last_admission(make_last_admission(routing))
        //max_queue(0)
    {
        for (const auto& [route, info] : routing) {
            queue.emplace(route, std::deque<std::shared_ptr<RPCMessage>>());
            services.push_back(route);
            if (config.is_ingress) {
                slo.set(route, info.slo.value());
                p95.set(route, -1);
                p50.set(route, -1);
            }
        }
    }

Ingress::~Ingress() {}

void Ingress::enqueue(std::shared_ptr<RPCMessage> rpc) {
    try {
        queue.at(rpc->get_service()).push_back(std::move(rpc));
    } catch (const std::out_of_range&) {
        LOG(FATAL) << "Service not found in ingress queue: " << rpc->get_service();
    } catch (const std::exception& e) {
        LOG(FATAL) << "Error in enqueueing RPC message: " << e.what()
                   << " service: " << rpc->get_service();
    }
}

std::shared_ptr<RPCMessage> Ingress::dequeue(std::string service) {
    if (queue.find(service) == queue.end()) {
        LOG(FATAL) << "Service not found in ingress queue: " << service;
    }
    if (queue.at(service).empty()) {
        LOG(FATAL) << "No RPC message in ingress queue for service: " << service;
    }
    auto rpc = std::move(queue.at(service).front());
    queue.at(service).pop_front();

    // update admission rate
    if (admission_rate.get(service).get_count() == 0) {
        last_admission.set(service, std::chrono::steady_clock::now());
    } else {
        auto now = std::chrono::steady_clock::now();
        int32_t time_diff_us = (int32_t)std::chrono::duration_cast<std::chrono::microseconds>(now - last_admission.get(service)).count();
        admission_rate.get(service).update(time_diff_us);
        last_admission.set(service, now);
    }

    return rpc;
}

bool Ingress::check_drop(RPCQueue& rpc_queue, RPCMapper& rpc_mapper, std::string& service, uint32_t limit) {
    int queue_size = (int)queue.at(service).size();
    if ((uint32_t)queue_size > limit ||
        ((int32_t)admission_rate.get(service).get_value() * queue_size > slo.get(service))) {
        auto drop_rpc = std::dynamic_pointer_cast<HTTPMessage>(std::move(queue.at(service).back()));
        if (!drop_rpc) {
            LOG(FATAL) << "Null pointer after dynamic_pointer_cast";
        }
        queue.at(service).pop_back();

        if (drop_rpc->get_service() != service) {
            LOG(FATAL) << "Service mismatch in drop RPC: expected " << service
                       << ", got " << drop_rpc->get_service();
        }

        drop_rpc->set_error(true);
        drop_rpc->set_status(503);

        // admit the failure response
        drop_id.add(drop_rpc->get_service(), 1);
        drop_rpc->set_us_fd(drop_fd);
        drop_rpc->set_us_stream_id(drop_id.get(drop_rpc->get_service()));
        rpc_mapper.route(ConnectionType::EGRESS, drop_rpc->get_ds_stream_id(),
                        drop_rpc->get_ds_fd(), drop_rpc->get_us_stream_id(), drop_fd);
        rpc_queue.enqueue(ConnectionType::EGRESS,
                        ConnectionDirection::UPSTREAM, // we want this to be forwarded to downstream connection
                        drop_rpc->get_us_fd(),
                        drop_rpc->get_us_stream_id());
        return true;
    }
    return false;
}

void Ingress::update_stats(int32_t new_p50, int32_t new_p95, std::string& service) {
    p50.set(service, new_p50);
    p95.set(service, new_p95);
    VLOG(1) << "Updated ingress p50 to " << new_p50 << " and p95 to " << new_p95 << " for service: " << service;
}

size_t  Ingress::size(std::string service) {
    return queue.at(service).size();
}
