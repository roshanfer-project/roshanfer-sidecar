#include "ingress.h"
#include "glog/logging.h"



Ingress::Ingress() :   queue() {}

Ingress::~Ingress() {}

void Ingress::enqueue(RPCMessage* rpc) {
    queue.push(rpc);
}

RPCMessage* Ingress::dequeue() {
    RPCMessage* rpc = queue.front();
    queue.pop();
    return rpc;
}
