#pragma once

#include <connection.h>
#include <unordered_map>
#include <memory>

class Listener {
    public:
        Listener(uint16_t, enum ConnectionType);
        int get_fd() { return fd; }
        uint16_t get_port() { return port; }
        HTTPConnection& add_connection(int fd, RPCMapper*, RPCQueue*, bool, struct hdr_histogram*);
        void remove_connection(int target_fd) { connections.erase(target_fd); }
        std::unordered_map<int, std::unique_ptr<HTTPConnection>>& get_connections() { return connections; }
        bool no_connections() { return connections.empty(); }
        std::string type_to_str();

    
    private:
        int fd; // local socket file descriptor
        uint16_t port;
        std::unordered_map<int, std::unique_ptr<HTTPConnection>> connections; // fd -> connection
        struct sockaddr_in addr;
    
    public:
        enum ConnectionType type; // type of the listener (INGRESS or EGRESS)
    };
