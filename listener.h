#pragma once

#include <connection.h>
#include <ring_wrapper.h>
#include <unordered_map>
#include <vector>
#include <memory>

class IngressListeners {
    public:
        IngressListeners();
        void add_listener(uint16_t port);
        void listen_all(RingWrapper& ring);
        TCPConnection& add_connection(int fd, uint16_t port);

    private:
        std::unordered_map<int, std::unique_ptr<Listener>> listeners;
};

class Listener {
public:
    Listener(uint16_t, enum ConnectionType);
    int get_fd() { return fd; }
    uint16_t get_port() { return port; }
    TCPConnection& add_connection(int fd);

private:
    int fd; // local socket file descriptor
    enum ConnectionType type;
    uint16_t port;
    std::vector<std::unique_ptr<TCPConnection>> connections;
};