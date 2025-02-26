#include <connection.h>
#include <fcntl.h>
#include "glog/logging.h"


ConnectionPool::ConnectionPool() 
    : connections(std::vector<TCPConnection>()) {};

void ConnectionPool::add_connection(int fd) {
    connections.push_back(TCPConnection(fd));
};

TCPConnection::TCPConnection(int fd) : fd(fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        LOG(FATAL) << "Failed to get flags for fd: " << fd;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        LOG(FATAL) << "Failed to set non-blocking for fd: " << fd;
    }
};

TCPConnection::~TCPConnection() {
    close(fd);
};