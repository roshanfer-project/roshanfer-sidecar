#pragma once

#include "connection_enums.h"
#include <cstdint>
#include <tuple>
#include <unordered_map>
#include <queue>

class RPCQueue {
    public:
        RPCQueue();
        void enqueue(ConnectionType, ConnectionDirection, int, uint32_t);
        std::tuple<int, uint32_t> dequeue(ConnectionType, ConnectionDirection);
        bool empty(ConnectionType, ConnectionDirection);
        int size(ConnectionType, ConnectionDirection);
    
    private:
        std::unordered_map<ConnectionType, 
            std::unordered_map<ConnectionDirection, std::queue<std::tuple<int, uint32_t>>>> queue_map;
};