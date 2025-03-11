#pragma once

#include <memory>
#include <vector>
#include "config.h"
#include "connection.h"
#include "buffer_manager.h"

class AddConnectionException : public std::runtime_error {
    public:
        AddConnectionException(std::unique_ptr<HTTPConnection>& conn)
         : std::runtime_error(""), conn(conn) {
        }
    
        std::unique_ptr<HTTPConnection>& conn;
};

class State {

    public:
        State(Config);
        /**
        @brief Routing for *requests*
        @note This should not be called for responses
        */
        int route(ConnectionType type);
        void add_buffer(Buffer* buffer);
        Buffer* get_buffer();
        bool has_buffer();
        std::unique_ptr<HTTPConnection>& get_connection(int fd) { return pool->get_connection(fd); }
        void remove_connection(int fd) { pool->remove_connection(fd); }


    private:
        std::unique_ptr<ConnectionPool> pool;
        std::vector<Buffer*> queue;
        Config config;

};