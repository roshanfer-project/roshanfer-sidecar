#pragma once


#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include <cstddef>
#include <cstdint>
#include <deque>


class Ingress {
    public:
        Ingress();
        ~Ingress();

        void enqueue(RPCMessage* rpc);
        RPCMessage* dequeue();
        size_t size() const { return queue.size(); }
        void update_p95(int64_t p95);
        bool check_drop(RPCQueue&, RPCMapper&);

    private:
        std::deque<RPCMessage*> queue;
        int64_t p95;
        int32_t drop_id;
};