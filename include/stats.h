#pragma once

#include "connection_enums.h"
#include "rpc_message.h"
#include <unordered_map>
#include "glog/logging.h"
#include "config.h"

class Stats {
    public:
        Stats();

    public:
        std::unordered_map<ConnectionType, int> sidecar_resp_in;
        bool new_response_in;
};

void inline report_latency(RPCMessage& rpc, ConnectionType type) {
    // calculate the duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - rpc.req_rcv_time);
    
    LOG(INFO) << "M# " << config.name << " E2E-" << type_to_str(type) << " " << duration.count();
    
    LOG(INFO) << "M# " << config.name << " REQ-FOR " << std::chrono::duration_cast<std::chrono::microseconds>(
        rpc.req_for_time - rpc.req_rcv_time).count();

    LOG(INFO) << "M# " << config.name << " RES-FOR " << std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - rpc.res_rcv_time).count();
}