#pragma once

//#include "NanoLog.h"
#include "connection_enums.h"
#include "fast_map.hpp"
#include "hdr/hdr_histogram.h"
#include "rpc_message.h"
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>
#include "config.h"
//#include "NanoLogCpp17.h"

//using namespace NanoLog::LogLevels;

class Utilization {
    public:
        Utilization(uint32_t, std::vector<std::string> services);
        void update(uint32_t, std::string&);
    
    private:
        void report(std::string& service);

    private:
        LocalMap<double> total;
        LocalMap<uint32_t> last_in;
        LocalMap<uint32_t> count;
        uint32_t period;
        LocalMap<std::chrono::steady_clock::time_point> last_update;
        LocalMap<std::chrono::steady_clock::time_point> last_report;
        struct hdr_histogram* hist;
};

void inline report_latency(const std::shared_ptr<RPCMessage>& rpc, ConnectionType /*type*/, struct hdr_histogram* hist) {
    // calculate the duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - rpc->req_rcv_time);
    
    // update hist only if we are Ingress and also just for E2E Ingress requests
    if (config.is_ingress) {
        bool ret = hdr_record_value(hist, static_cast<int64_t>(duration.count()));
        if (!ret) {
            LOG(FATAL) << "Failed to record value: " << static_cast<int64_t>(duration.count());
        }
    }
    
    /* if (!rpc.is_error()) {
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
        if (rpc.http() == HTTP::HTTP1) {

            NANO_LOG(NOTICE, "M# %s DROP %s %s:%d 1",
                config.name.c_str(), type_to_str(type).c_str(), rpc.get_service().c_str(), static_cast<HTTPMessage*>(&rpc)->get_status());
        } else {
            NANO_LOG(NOTICE, "M# %s DROP %s %s:%s 1",
                config.name.c_str(), type_to_str(type).c_str(), rpc.get_service().c_str(), rpc.get_method().c_str());
        }
        
    } */
    
    
}

class MovingAverage {
    public:
        MovingAverage();

        // delete copy semantics
        MovingAverage(const MovingAverage&) = delete;
        MovingAverage& operator=(const MovingAverage&) = delete;

        // delete move semantics
        MovingAverage(MovingAverage&&) = delete;
        MovingAverage& operator=(MovingAverage&&) = delete;
    
        void update(int32_t);
        float get_value();
        uint32_t get_count() const;
        void set_description(std::string desc) { description = desc; }

    private:
        uint32_t count;
        float value;
        std::string description;
};

class Stats {
    public:
        Stats(std::vector<std::string> services);

        // delete copy semantics
        Stats(const Stats&) = delete;
        Stats& operator=(const Stats&) = delete;

        // delete move semantics
        Stats(Stats&&) = delete;
        Stats& operator=(Stats&&) = delete;
        
        void report_latency(const std::shared_ptr<RPCMessage>& rpc);
        void update_hist(struct hdr_histogram*);
        LocalMap<MovingAverage>& get_avg_service_time_us() { return avg_service_time_us; }
    
    private:
        struct hdr_histogram* hist;
        LocalMap<MovingAverage> avg_service_time_us;
};