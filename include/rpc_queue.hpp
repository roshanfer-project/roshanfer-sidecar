#pragma once

#include "connection_enums.hpp"
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <unordered_map>
#include <queue>

class RPCQueue {
    public:
        RPCQueue();
        void enqueue(ConnectionType, ConnectionDirection, int, int32_t);
        std::tuple<int, int32_t> dequeue(ConnectionType, ConnectionDirection);
        bool empty(ConnectionType, ConnectionDirection);
        size_t size(ConnectionType, ConnectionDirection);
    
    private:
        std::unordered_map<ConnectionType, 
            std::unordered_map<ConnectionDirection, std::queue<std::tuple<int, int32_t>>>> queue_map;
};