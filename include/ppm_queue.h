#pragma once

#include "config.h"
#include "rpc_message.h"
#include <queue>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unordered_map>
#include "utils.h"




class PPMQueue {
    public:
        PPMQueue(std::unordered_map<std::string, RoutingEntry, TransparentHash, TransparentEqual> routing);
        void enqueue(RPCMessage*);
        RPCMessage* dequeue(const std::string&);
        // checks if the service exists and the queue is not empty
        const std::string& check(std::string_view&);
        int get_fd(const std::string&);
        size_t size(const std::string&);
    
    private:
        std::unordered_map<std::string, std::queue<RPCMessage*>,
        TransparentHash, TransparentEqual> ppm_queue;
};