#pragma once

#include <connection.h>
#include <unordered_map>
#include <memory>

class Listener {
    public:
        Listener(uint16_t, enum ConnectionType);
        int get_fd() { return fd; }
        uint16_t get_port() { return port; }
        HTTPConnection& add_connection(int fd);
        void remove_connection(int fd) { connections.erase(fd); }
        std::unordered_map<int, std::unique_ptr<HTTPConnection>>& get_connections() { return connections; }
        bool no_connections() { return connections.empty(); }

    
    private:
        int fd; // local socket file descriptor
        enum ConnectionType type;
        uint16_t port;
        std::unordered_map<int, std::unique_ptr<HTTPConnection>> connections; // fd -> connection
    };
