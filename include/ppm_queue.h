#pragma once

#include "rpc_message.h"
#include <memory>
#include <queue>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unordered_map>


struct TransparentHash {
    using is_transparent = void; // important for heterogeneous lookup
    std::size_t operator()(std::string_view txt) const noexcept {
        return std::hash<std::string_view>{}(txt);
    }
};

struct TransparentEqual {
    using is_transparent = void;
    bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
        return lhs == rhs;
    }
};

class PPMQueue {
    public:
        PPMQueue();
        void enqueue(std::shared_ptr<RPCMessage>);
        std::shared_ptr<RPCMessage> dequeue(const std::string&);
        const std::string& check(std::string_view&);
        int get_fd(const std::string&);
    
    private:
        std::unordered_map<std::string, std::queue<std::shared_ptr<RPCMessage>>,
        TransparentHash, TransparentEqual> ppm_queue;
};