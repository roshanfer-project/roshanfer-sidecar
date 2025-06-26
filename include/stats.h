#pragma once

//#include "NanoLog.h"
#include "connection_enums.h"
#include "hdr/hdr_histogram.h"
#include "rpc_message.h"
#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include "config.h"
//#include "NanoLogCpp17.h"

//using namespace NanoLog::LogLevels;

class Stats {
    public:
        Stats(std::vector<RoutingEntry> routing);

    public:
        std::unordered_map<ConnectionType, int> sidecar_resp_in;
        std::unordered_map<std::string, bool> new_response_in;
};

void inline report_latency(RPCMessage& rpc, ConnectionType type, struct hdr_histogram* hist) {
    // calculate the duration
    /* auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - rpc.req_rcv_time);
    
    // update hist only if we are Ingress and also just for E2E Ingress requests
    if (config.is_ingress == 1 && type == ConnectionType::INGRESS) {
        hdr_record_value(hist, static_cast<int64_t>(duration.count()));
    }
    
    if (!rpc.is_error()) {
        // template: M# <sidecar name> <metric name> <connection type> <service>:<method> <value>
        NANO_LOG(NOTICE, "M# %s E2E %s %s:%s %ld",
            config.name.c_str(), type_to_str(type).c_str(), rpc.get_service().c_str(), rpc.get_method().c_str(), duration.count());
        
        NANO_LOG(NOTICE, "M# %s REQ-FOR %s %s:%s %ld",
            config.name.c_str(), type_to_str(type).c_str(), rpc.get_service().c_str(), rpc.get_method().c_str(), 
            std::chrono::duration_cast<std::chrono::microseconds>(rpc.req_for_time - rpc.req_rcv_time).count());

        NANO_LOG(NOTICE, "M# %s RES-FOR %s %s:%s %ld",
            config.name.c_str(), type_to_str(type).c_str(), rpc.get_service().c_str(), rpc.get_method().c_str(), 
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - rpc.res_rcv_time).count());
    } else {
        if (rpc.is_http()) {

            NANO_LOG(NOTICE, "M# %s DROP %s %s:%d 1",
                config.name.c_str(), type_to_str(type).c_str(), rpc.get_service().c_str(), static_cast<HTTPMessage*>(&rpc)->get_status());
        } else {
            NANO_LOG(NOTICE, "M# %s DROP %s %s:%s 1",
                config.name.c_str(), type_to_str(type).c_str(), rpc.get_service().c_str(), rpc.get_method().c_str());
        }
        
    } */
    
    
}