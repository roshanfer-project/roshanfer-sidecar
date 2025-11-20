#pragma once

#include "buffer.h"
#include "connection.h"
#include "listener.h"
//#include "rpc_message.h"
#include <cassert>
#include <cstddef>
#include <memory>
#include <string>

enum class Operation {
    ACCEPT,
    READ,
    WRITE,
    CONNECT,
    CANCEL,
    RCVMSG,
    SENDMSG,
    CLEAR
};

inline std::string operation_to_str(Operation op) {
    switch (op) {
        case Operation::ACCEPT: return "ACCEPT";
        case Operation::READ: return "READ";
        case Operation::WRITE: return "WRITE";
        case Operation::CONNECT: return "CONNECT";
        case Operation::CANCEL: return "CANCEL";
        case Operation::RCVMSG: return "RCVMSG";
        case Operation::SENDMSG: return "SENDMSG";
        case Operation::CLEAR: return "CLEAR";
        default: return "UNKNOWN";
    }
}

enum class UDPType {
    REQUEST,
    RESPONSE,
    CLEAR
};

inline std::string udp_type_to_str(UDPType type) {
    switch (type) {
        case UDPType::REQUEST: return "REQUEST";
        case UDPType::RESPONSE: return "RESPONSE";
        case UDPType::CLEAR: return "CLEAR";
        default: return "UNKNOWN";
    }
}

class UserData {
    public:
        UserData(size_t);
        ~UserData();

        // delete copy semantics
        UserData(const UserData&) = delete;
        UserData& operator=(const UserData&) = delete;

        // delete move semantics
        UserData(UserData&&) = delete;
        UserData& operator=(UserData&&) = delete;

        std::unique_ptr<Buffer> get_buffer();
        void set_buffer(std::unique_ptr<Buffer>);
        void clear();
    
    private:
        std::unique_ptr<Buffer> buffer;
    
    public:
        bool in_ring;
        std::shared_ptr<Listener> listener;
        std::shared_ptr<HTTPConnection> conn;
        enum Operation op;
        size_t index;
        UDPType udp_type;
        //std::unique_ptr<RPCMessage> rpc_message;
        std::unique_ptr<struct sockaddr_in> accept_addr; // used for preparing accept
};

void inline prepare_read(UserData* ud, std::unique_ptr<Buffer> buffer,
     std::shared_ptr<Listener> listener, std::shared_ptr<HTTPConnection> conn) {
    ud->set_buffer(std::move(buffer));
    ud->op = Operation::READ;
    ud->conn = std::move(conn);
    ud->listener = std::move(listener);
}

void inline prepare_write(UserData* ud, std::unique_ptr<Buffer> buffer, std::shared_ptr<HTTPConnection> conn) {
    ud->set_buffer(std::move(buffer));
    ud->op = Operation::WRITE;
    ud->conn = std::move(conn);
}