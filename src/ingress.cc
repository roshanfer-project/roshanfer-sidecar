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

Ingress::Ingress(std::unordered_map<std::string, RoutingEntry, TransparentHash, TransparentEqual> routing, int index_arg) 
    :   queue(std::unordered_map<std::string, std::deque<std::shared_ptr<RPCMessage>>>()),
        drop_id(make_drop_id(routing)),
        services(std::vector<std::string>(routing.size())),
        drop_fd(-index_arg),
        p95_us(make_drop_id(routing)),
        p50_us(make_drop_id(routing)),
        slo_us(make_drop_id(routing)),
        last_rpc_id(0)
        //max_queue(0)
    {
        for (const auto& [route, info] : routing) {
            queue.emplace(route, std::deque<std::shared_ptr<RPCMessage>>());
            services.push_back(route);
            if (config.is_ingress) {
                slo_us.set(route, info.slo.value() * 1000);
                p95_us.set(route, -1);
                p50_us.set(route, -1);
            }
        }
    }

Ingress::~Ingress() {}

void Ingress::enqueue(std::shared_ptr<RPCMessage> rpc) {
    try {
        VLOG(2) << "Enqueued RPC message for service: " << rpc->get_service();
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

    VLOG(2) << "Dequeued RPC message for service: " << service;
    return rpc;
}

void Ingress::add_rpc_id_header(std::shared_ptr<RPCMessage>& rpc) {
    // convert last_rpc_id to a char array
    // reset the array
    rpc_id_header_value.fill(0);
    // update last_rpc_id and convert to a char array
    last_rpc_id++;
    // convert last_rpc_id to a char array
    std::snprintf(rpc_id_header_value.data(), rpc_id_header_value.size(), "%d", last_rpc_id);
    rpc->add_header_field(
        RPC_ID_HEADER_NAME, RPC_ID_HEADER_NAME_LEN,
        reinterpret_cast<const uint8_t*>(rpc_id_header_value.data()), rpc_id_header_value.size(),
        true, false
    );
}

int64_t Ingress::add_to_be_admitted_or_drop(RPCQueue& rpc_queue, RPCMapper& rpc_mapper,
     std::string& service, int64_t extra_slot_ingress, int32_t queueing_delay) {

    int new_requests = (int)queue.at(service).size();
    int32_t current_slo_us = slo_us.get(service);
    int new_added = 0;
    
    while (extra_slot_ingress > 0 && new_requests > 0 && queueing_delay < current_slo_us) {
        auto rpc = dequeue(service);
        add_rpc_id_header(rpc);
        rpc_queue.enqueue(
            ConnectionType::EGRESS,
            ConnectionDirection::DOWNSTREAM,
            rpc->get_ds_fd(),
            rpc->get_ds_stream_id()
        );
        extra_slot_ingress--;
        new_added++;
        new_requests--;
        VLOG(2) << "INGRESS: New request to be admitted "
                << "| service: " << service
                << "| id: " << rpc->get_id()
                << "| extra slot: " << extra_slot_ingress
                << "| queueing delay: " << queueing_delay;
    }

    // drop remaining requests
    while (new_requests > 0) {
        auto drop_rpc = std::dynamic_pointer_cast<HTTPMessage>(std::move(queue.at(service).back()));
            if (!drop_rpc) {
                LOG(FATAL) << "Null pointer after dynamic_pointer_cast"
                        << " service: " << service
                        << " queue size: " << queue.at(service).size();
            }
            queue.at(service).pop_back();

            if (drop_rpc->get_service() != service) {
                LOG(FATAL) << "Service mismatch in drop RPC: expected " << service
                        << ", got " << drop_rpc->get_service();
            }

            drop_rpc->set_error(true);
            drop_rpc->set_status(503);
            drop_id.add(drop_rpc->get_service(), 1);
            drop_rpc->set_us_fd(drop_fd);
            drop_rpc->set_us_stream_id(drop_id.get(drop_rpc->get_service()));
            rpc_mapper.route(ConnectionType::EGRESS, drop_rpc->get_ds_stream_id(),
                            drop_rpc->get_ds_fd(), drop_rpc->get_us_stream_id(), drop_fd);
            rpc_queue.enqueue(ConnectionType::EGRESS,
                            ConnectionDirection::UPSTREAM, // we want this to be forwarded to downstream connection
                            drop_rpc->get_us_fd(),
                            drop_rpc->get_us_stream_id());

            VLOG(2) << "INGRESS: Dropped request "
                    << "| service: " << drop_rpc->get_service()
                    << "| id: " << drop_rpc->get_id()
                    << "| extra slot: " << extra_slot_ingress
                    << "| queueing delay: " << queueing_delay;
            new_requests--;
    }

    return new_added;
}

void Ingress::update_stats(int32_t new_p50_us, int32_t new_p95_us, std::string& service) {
    p50_us.set(service, new_p50_us);
    p95_us.set(service, new_p95_us);
    VLOG(1) << "Ingress: Updated p50_us to " << new_p50_us << " and p95_us to " << new_p95_us << " for service: " << service;
}

size_t  Ingress::size(std::string service) {
    return queue.at(service).size();
}

void Ingress::dump_state() {
    LOG(INFO) << "--- Ingress Queue Sizes (ingress) ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << queue.at(route).size();
    }
    LOG(INFO) << "--- Drop ID (drop_id) ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << drop_id.get(route);
    }
    LOG(INFO) << "--- P95 US (p95_us) ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << p95_us.get(route);
    }
    LOG(INFO) << "--- P50 US (p50_us) ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << p50_us.get(route);
    }
    LOG(INFO) << "--- SLO US (slo_us) ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << slo_us.get(route);
    }
}
