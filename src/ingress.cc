#include "ingress.h"
#include "connection_enums.h"
#include "glog/logging.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include <cstdint>
#include <deque>



Ingress::Ingress() :   queue(std::deque<RPCMessage*>()), drop_id(0) {}

Ingress::~Ingress() {}

void Ingress::enqueue(RPCMessage* rpc) {
    queue.push_back(rpc);
}

RPCMessage* Ingress::dequeue() {
    RPCMessage* rpc = queue.front();
    queue.pop_front();
    return rpc;
}

bool Ingress::check_drop(RPCQueue& rpc_queue, RPCMapper& rpc_mapper) {
    if (queue.size() > 10) {
        auto last_req_wait = static_cast<int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - queue.front()->req_rcv_time).count());
        VLOG(1) << "SLO violation prevention budget: " << 10*p95 - last_req_wait;

        // drop the last request
        auto drop_rpc = static_cast<HTTPMessage*>(queue.back());
        drop_rpc->set_error(true);
        drop_rpc->set_status(503);
        queue.pop_back();

        // admit the failure response
        drop_id++;
        drop_rpc->set_us_fd(-1);
        drop_rpc->set_us_stream_id(drop_id);
        rpc_mapper.route(ConnectionType::INGRESS, drop_rpc->get_ds_stream_id(),
                           drop_rpc->get_ds_fd(), drop_rpc->get_us_stream_id(), -1);
        rpc_queue.enqueue(ConnectionType::INGRESS,
                           ConnectionDirection::UPSTREAM, // we want this to be forwarded to downstream connection
                           drop_rpc->get_us_fd(),
                           drop_rpc->get_us_stream_id());
        return true;
    }
    else 
        return false;
}

void Ingress::update_p95(int64_t p95) {
    this->p95 = p95;
    VLOG(1) << "Updated ingress p95 to " << p95;
}
