#pragma once

#include "connection_enums.h"
#include "stats.h"
#include <connection.h>
#include <unordered_map>
#include <memory>

class Listener {
    public:
        Listener(uint16_t, enum ConnectionType);
        ~Listener();

        // delete copy semantics
        Listener(const Listener&) = delete;
        Listener& operator=(const Listener&) = delete;

        // delete move semantics
        Listener(Listener&&) = delete;
        Listener& operator=(Listener&&) = delete;

        int get_fd() { return fd; }
        uint16_t get_port() { return port; }
        std::shared_ptr<HTTPConnection> add_connection(int fd, RPCMapper*, RPCQueue*, HTTP, Stats*);
        void remove_connection(int target_fd) { connections.erase(target_fd); }
        std::shared_ptr<HTTPConnection> get_connection(int);
        bool no_connections() { return connections.empty(); }
        std::string type_to_str();
        void dump_connections();

    
    private:
        int fd; // local socket file descriptor
        uint16_t port;
        std::unordered_map<int, std::shared_ptr<HTTPConnection>> connections; // fd -> connection
        struct sockaddr_in addr;
    
    public:
        enum ConnectionType type; // type of the listener (INGRESS or EGRESS)
    };
