#pragma once

#include <connection.h>
#include <unordered_map>
#include <memory>

class Listener {
    public:
        Listener(uint16_t, enum ConnectionType);
        int get_fd() { return fd; }
        uint16_t get_port() { return port; }
        TCPConnection& add_connection(int fd);
        void remove_connection(int fd) { connections.erase(fd); }
        std::unordered_map<int, std::unique_ptr<TCPConnection>>& get_connections() { return connections; }

    
    private:
        int fd; // local socket file descriptor
        enum ConnectionType type;
        uint16_t port;
        std::unordered_map<int, std::unique_ptr<TCPConnection>> connections; // fd -> connection
    };
