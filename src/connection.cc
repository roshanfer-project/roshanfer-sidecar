#include <connection.h>
#include <fcntl.h>
#include <liburing.h>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "glog/logging.h"
#include "http2_parser.h"
#include <memory>
#include <sys/types.h>
#include <unordered_map>



ConnectionPool::ConnectionPool() 
    : connections(std::unordered_map<int, std::unique_ptr<HTTPConnection>>()) {};

std::unique_ptr<HTTPConnection>& ConnectionPool::add_connection(std::string&& host, int port) {
    auto c = std::make_unique<HTTPConnection>(host, port);
    int fd = c->get_fd();
    connections[fd] = std::move(c);
    return connections[fd];
};

bool ConnectionPool::has_connection(int fd) {
    return connections.find(fd) != connections.end();
};

std::unique_ptr<HTTPConnection>& ConnectionPool::get_any_connection() {
    if (connections.empty()) {
        throw NoConnectionException();
    }
    return connections.begin()->second;
};

HTTPConnection::HTTPConnection(int fd) 
    :   fd(fd),
        type(ConnectionType::INGRESS),
        addr(0),
        parser() {
    
    // set non-blocking
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        LOG(FATAL) << "Failed to get flags for fd: " << fd;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        LOG(FATAL) << "Failed to set non-blocking for fd: " << fd;
    }
};

HTTPConnection::HTTPConnection(std::string host, int port) 
    :   type(ConnectionType::EGRESS),
        addr(0),
        fd(0),
        parser() {
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

sockaddr* HTTPConnection::get_addr() {
    if (type == ConnectionType::INGRESS) {
        LOG(FATAL) << "Cannot get address for ingress connection";
    }
    return reinterpret_cast<sockaddr*>(&addr);
}

HTTPConnection::~HTTPConnection() {
    DLOG(INFO) << "Closing connection on fd: " << fd;
    close(fd);
};