#include <connection.h>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

ConnectionPool::ConnectionPool() 
    : connections(std::vector<TCPConnection>()) {};

void ConnectionPool::add_connection(int fd) {
    connections.push_back(TCPConnection(fd));
};

TCPConnection::TCPConnection(int fd) : fd(fd) {};