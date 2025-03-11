#pragma once

#include <memory>
#include <netinet/in.h>
#include <span>
#include <sys/socket.h>
#include <sys/types.h>
#include <unordered_map>
#include <string>
#include <vector>
#include "http2_parser.h"

enum ConnectionType {
    INGRESS,
    EGRESS,
};

class NoConnectionException : public std::runtime_error {
    public:
        NoConnectionException() : std::runtime_error("No connection available") {}
};

class HTTPConnection {

    public:
        /**
         * @brief Construct an endpoint connection
         */
        HTTPConnection(std::string host, int port);

        /*
         * @brief Construct an egress connection
         */
        HTTPConnection(int fd); 
        ~HTTPConnection();
        int get_fd() { return fd; }
        sockaddr* get_addr();
        std::vector<HTTP2Frame> parse(std::span<const char> input) { return parser.parse(input); }

    private:
        int fd; // local socket file descriptor
        sockaddr_in addr;
        HTTP2Parser parser;

    public:
        ConnectionType type;

};

class ConnectionPool {

    public:
    ConnectionPool();

        /**
         * @brief Add a connection to the pool
         */
        std::unique_ptr<HTTPConnection>& add_connection(std::string&& host, int port);
        std::unique_ptr<HTTPConnection>& get_connection(int fd) { return connections[fd]; }
        std::unique_ptr<HTTPConnection>& get_any_connection();
        bool has_connection(int fd);
        void remove_connection(int fd) { connections.erase(fd); }
    
    private:
        std::unordered_map<int, std::unique_ptr<HTTPConnection>> connections; // fd: connection
};