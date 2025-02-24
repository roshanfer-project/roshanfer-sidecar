#include <listener.h>
#include <connection.h>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <memory>


IngressListeners::IngressListeners()
    : listeners(std::unordered_map<int, std::unique_ptr<Listener>>()) {};

void IngressListeners::add_listener(uint16_t port) {
    listeners[port] = std::make_unique<Listener>(port, ConnectionType::INGRESS);
};

Listener::Listener(uint16_t port, ConnectionType type) 
    : port(port), type(type) {
    // Create a socket
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    // Bind the socket
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        throw std::runtime_error("Failed to bind socket");
    }

    // Listen on the socket
    if (listen(fd, 10) < 0) {
        throw std::runtime_error("Failed to listen on socket");
    }
};

TCPConnection&Listener::add_connection(int fd) {
    connections.push_back(std::make_unique<TCPConnection>(fd));
    return *connections.back();
};

void IngressListeners::listen_all(RingWrapper& ring) {
    for (auto& listener : listeners) {
        ring.prepare_accept(*listener.second.get());
    }
};

TCPConnection& IngressListeners::add_connection(int fd, uint16_t port) {
    return listeners[port]->add_connection(fd);
};

