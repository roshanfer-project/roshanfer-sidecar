#pragma once


#include "config.h"
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
        Ingress(std::vector<RoutingEntry>);
        ~Ingress();

        void enqueue(RPCMessage* rpc);
        RPCMessage* dequeue(std::string);
        size_t size(std::string);
        void update_p95(int64_t p95);
        bool check_drop(RPCQueue&, RPCMapper&);
    
    private:
        RPCMessage* select_drop();


    private:
        std::unordered_map<std::string, std::deque<RPCMessage*>> queue;
        int64_t p95;
        int32_t drop_id;
        size_t total_size;
};