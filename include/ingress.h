#pragma once


#include "config.h"
#include "fast_map.hpp"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>


class Ingress {
    public:
        Ingress(std::unordered_map<std::string, RoutingEntry, TransparentHash, TransparentEqual>, int);
        ~Ingress();

        void enqueue(std::shared_ptr<RPCMessage> rpc);
        std::shared_ptr<RPCMessage> dequeue(std::string);
        size_t size(std::string);
        void update_stats(int32_t, int32_t, std::string&);
        bool check_drop(RPCQueue&, RPCMapper&, std::string&, uint32_t);


    private:
        std::unordered_map<std::string, std::deque<std::shared_ptr<RPCMessage>>> queue;
        LocalMap<int32_t> drop_id;
        std::vector<std::string> services;
        int drop_fd;
        LocalMap<int32_t> p95;
        LocalMap<int32_t> p50;
        LocalMap<int32_t> slo;
        LocalMap<MovingAverage> admission_rate;
        LocalMap<std::chrono::time_point<std::chrono::steady_clock>> last_admission;
        //int32_t max_queue;
};