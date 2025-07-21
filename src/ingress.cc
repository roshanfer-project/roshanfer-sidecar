#include "ingress.h"
#include "connection_enums.h"
#include "glog/logging.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include <cstddef>
#include <cstdint>
#include <deque>



Ingress::Ingress(std::vector<RoutingEntry> routing) 
    : queue(std::unordered_map<std::string, std::deque<RPCMessage*>>()), drop_id(0), total_size(0)
    {
        for (const auto& entry : routing) {
            queue.emplace(entry.service, std::deque<RPCMessage*>());
        }
    }

Ingress::~Ingress() {}

void Ingress::enqueue(RPCMessage* rpc) {
    try {
        queue.at(rpc->get_service()).push_back(rpc);
        total_size++;
    } catch (const std::out_of_range&) {
        LOG(FATAL) << "Service not found in ingress queue: " << rpc->get_service();
    } catch (const std::exception& e) {
        LOG(FATAL) << "Error in enqueueing RPC message: " << e.what()
                   << " service: " << rpc->get_service();
    }
}

RPCMessage* Ingress::dequeue() {
    if (!queue.at("hotels").empty()) {
        RPCMessage* rpc = queue.at("hotels").front();
        queue.at("hotels").pop_front();
        total_size--;
        return rpc;
    } else if (!queue.at("reservation").empty()) {
        RPCMessage* rpc = queue.at("reservation").front();
        queue.at("reservation").pop_front();
        total_size--;
        return rpc;
    } else {
        // log the content of the queue
        for (const auto& kv : queue) {
            LOG(INFO) << "Service: " << kv.first << ", size: " << kv.second.size();
        }
        LOG(FATAL) << "No RPC message in ingress queue";
    }
}

RPCMessage* Ingress::select_drop() {
    // drop an RPC from "reservation" queue if possible
    if (!queue.at("reservation").empty()) {
        RPCMessage* rpc = queue.at("reservation").back();
        queue.at("reservation").pop_back();
        total_size--;
        return rpc;
    } else if (!queue.at("hotels").empty()) {
        RPCMessage* rpc = queue.at("hotels").back();
        queue.at("hotels").pop_back();
        total_size--;
        return rpc;
    } else {
        LOG(FATAL) << "No RPC message in ingress queue to drop";
    }
}

bool Ingress::check_drop(RPCQueue& rpc_queue, RPCMapper& rpc_mapper) {
    if (this->size() > 10) {
        /* auto last_req_wait = static_cast<int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - queue.front()->req_rcv_time).count());
        VLOG(1) << "SLO violation prevention budget: " << 10*p95 - last_req_wait; */

        // drop the last request
        auto drop_rpc = dynamic_cast<HTTPMessage*>(select_drop());
        drop_rpc->set_error(true);
        drop_rpc->set_status(503);;

        // admit the failure response
        drop_id++;
        drop_rpc->set_us_fd(-1);
        drop_rpc->set_us_stream_id(drop_id);
        rpc_mapper.route(ConnectionType::EGRESS, drop_rpc->get_ds_stream_id(),
                           drop_rpc->get_ds_fd(), drop_rpc->get_us_stream_id(), -1);
        rpc_queue.enqueue(ConnectionType::EGRESS,
                           ConnectionDirection::UPSTREAM, // we want this to be forwarded to downstream connection
                           drop_rpc->get_us_fd(),
                           drop_rpc->get_us_stream_id());
        return true;
    }
    else 
        return false;
}

void Ingress::update_p95(int64_t new_p95) {
    this->p95 = new_p95;
    VLOG(1) << "Updated ingress p95 to " << p95;
}

size_t  Ingress::size() {
    return total_size;
}
