#pragma once


#include "config.h"
#include "fast_map.hpp"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
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

        void enqueue(RPCMessage* rpc);
        RPCMessage* dequeue(std::string);
        size_t size(std::string);
        void update_stats(int32_t, int32_t, std::string&);
        bool check_drop(RPCQueue&, RPCMapper&, std::string&, uint32_t);


    private:
        std::unordered_map<std::string, std::deque<RPCMessage*>> queue;
        LocalMap<int32_t> drop_id;
        std::vector<std::string> services;
        int drop_fd;
        LocalMap<int32_t> p95;
        LocalMap<int32_t> p50;
        LocalMap<int32_t> slo;
};