#pragma once

#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unordered_map>
#include <string>

enum class ConnectionType {
    INGRESS,
    EGRESS,
};

class NoConnectionException : public std::runtime_error {
    public:
        NoConnectionException() : std::runtime_error("No connection available") {}
};

class TCPConnection {

    public:
        /**
         * @brief Construct an endpoint connection
         */
        TCPConnection(std::string host, int port);

        /**
         * @brief Construct an egress connection
         */
        TCPConnection(int fd); 
        ~TCPConnection();
        int get_fd() { return fd; }
        sockaddr* get_addr();

    private:
        int fd; // local socket file descriptor
        sockaddr_in addr;

    public:
        ConnectionType type;

};

class ConnectionPool {

    public:
    ConnectionPool();

        /**
         * @brief Add a connection to the pool
         */
        std::unique_ptr<TCPConnection>& add_connection(std::string&& host, int port);
        std::unique_ptr<TCPConnection>& get_connection(int fd) { return connections[fd]; }
        std::unique_ptr<TCPConnection>& get_any_connection();
        bool has_connection(int fd);
        void remove_connection(int fd) { connections.erase(fd); }
    
    private:
        std::unordered_map<int, std::unique_ptr<TCPConnection>> connections; // fd: connection
};