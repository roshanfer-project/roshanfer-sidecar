#include <connection.h>
#include <fcntl.h>
#include <liburing.h>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "glog/logging.h"
#include <memory>
#include <unordered_map>



ConnectionPool::ConnectionPool() 
    : connections(std::unordered_map<int, std::unique_ptr<TCPConnection>>()) {};

std::unique_ptr<TCPConnection>& ConnectionPool::add_connection(std::string&& host, int port) {
    auto c = std::make_unique<TCPConnection>(host, port);
    int fd = c->get_fd();
    connections[fd] = std::move(c);
    return connections[fd];
};

bool ConnectionPool::has_connection(int fd) {
    return connections.find(fd) != connections.end();
};

std::unique_ptr<TCPConnection>& ConnectionPool::get_any_connection() {
    if (connections.empty()) {
        throw NoConnectionException();
    }
    return connections.begin()->second;
};

TCPConnection::TCPConnection(int fd) 
    :   fd(fd),
        type(ConnectionType::INGRESS),
        addr(0) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        LOG(FATAL) << "Failed to get flags for fd: " << fd;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        LOG(FATAL) << "Failed to set non-blocking for fd: " << fd;
    }
};

TCPConnection::TCPConnection(std::string host, int port) 
    :   type(ConnectionType::EGRESS),
        addr(0),
        fd(0) {
    fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        LOG(FATAL) << "Invalid address: " << host;
    }
}

sockaddr* TCPConnection::get_addr() {
    if (type == ConnectionType::INGRESS) {
        LOG(FATAL) << "Cannot get address for ingress connection";
    }
    return reinterpret_cast<sockaddr*>(&addr);
}

TCPConnection::~TCPConnection() {
    close(fd);
};