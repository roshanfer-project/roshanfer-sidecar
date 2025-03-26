#pragma once

#include "buffer.h"
#include "connection.h"
#include "listener.h"
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
    Buffer* buffer;
    Listener* listener;
    HTTPConnection* conn;
    enum Operation op;
    int index;
    UDPType udp_type;
    std::unique_ptr<RPCMessage> rpc_message;
};

void inline prepare_read(UserData* ud, Buffer* buffer,
     Listener* listener, HTTPConnection* conn) {
    ud->buffer = buffer;
    ud->op = Operation::READ;
    ud->conn = conn;
    ud->listener = listener;
}

void inline prepare_write(UserData* ud, Buffer* buffer, HTTPConnection* conn) {
    ud->buffer = buffer;
    ud->op = Operation::WRITE;
    ud->conn = conn;
}