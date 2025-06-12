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

void Ingress::update_p95(int64_t p95) {
    this->p95 = p95;
    VLOG(1) << "Updated ingress p95 to " << p95;
}
