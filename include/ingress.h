#pragma once


#include "rpc_message.h"


class Ingress {
    public:
        Ingress();
        ~Ingress();

        void enqueue(RPCMessage* rpc);
        RPCMessage* dequeue();
        int size() const { return queue.size(); }

    private:
        std::queue<RPCMessage*> queue;
};