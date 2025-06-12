#pragma once


#include "rpc_message.h"
#include <cstdint>


class Ingress {
    public:
        Ingress();
        ~Ingress();

        void enqueue(RPCMessage* rpc);
        RPCMessage* dequeue();
        int size() const { return queue.size(); }
        void update_p95(int64_t p95);

    private:
        std::queue<RPCMessage*> queue;
        int64_t p95;
};