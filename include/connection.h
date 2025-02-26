#pragma once

#include <vector>

class TCPConnection {

    public:
        ///**
        // * @brief Construct an egress connection
        // */
        //TCPConnection(uint16_t port, const std::string& ip);

        /**
         * @brief Construct an ingress connection
         */
        TCPConnection(int);
        ~TCPConnection();
        int get_fd() { return fd; }

    private:
        int fd; // local socket file descriptor

};

class ConnectionPool {

    public:
    ConnectionPool();

        /**
         * @brief Add a connection to the pool
         */
        void add_connection(int fd);
    
    private:
        std::vector<TCPConnection> connections;
};

enum class ConnectionType {
    INGRESS,
    EGRESS,
};