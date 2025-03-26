#pragma once

#include "connection_enums.h"
#include <cstdint>
#include <unordered_map>
#include <queue>

class RPCQueue {
    public:
        RPCQueue();
        void enqueue(ConnectionType, ConnectionDirection, uint32_t);
        uint32_t dequeue(ConnectionType, ConnectionDirection);
        bool empty(ConnectionType, ConnectionDirection);
    
    private:
        std::unordered_map<ConnectionType, 
            std::unordered_map<ConnectionDirection, std::queue<uint32_t>>> queue_map;
};