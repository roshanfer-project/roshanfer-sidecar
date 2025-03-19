#pragma once

#include "rpc_message.h"
#include <memory>

enum class Operation {
    ACCEPT,
    READ,
    WRITE,
    CONNECT,
    CANCEL,
    RCVMSG,
    SENDMSG
};

enum class UDPType {
    REQUEST,
    RESPONSE
};

struct UserData {
    void* data;
    enum Operation op;
    int index;
    UDPType udp_type;
    std::unique_ptr<RPCMessage> rpc_message;
};