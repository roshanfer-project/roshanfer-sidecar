#pragma once

#include <memory>
#include <vector>
#include "config.h"
#include "connection.h"
#include "buffer_manager.h"
#include <unordered_map>

class AddConnectionException : public std::runtime_error {
    public:
        AddConnectionException(std::unique_ptr<HTTPConnection>& conn)
         : std::runtime_error(""), conn(conn) {
        }
    
        std::unique_ptr<HTTPConnection>& conn;
};

class ConnectionNotUPException: public std::runtime_error {
    public:
        ConnectionNotUPException(std::unique_ptr<HTTPConnection>& conn) 
            : std::runtime_error(""), conn(conn) {}


        std::unique_ptr<HTTPConnection>& conn;
};

class State {

    public:
        State(Config);
        /**
        @brief Routing for *requests*
        @note This should not be called for responses
        */
        int route(ConnectionType);
        void add_buffer(Buffer*, ConnectionType);
        Buffer* get_buffer(ConnectionType);
        bool has_buffer(ConnectionType);
        std::unique_ptr<HTTPConnection>& get_connection(int, ConnectionType);
        void remove_connection(int, ConnectionType);


    private:
        std::unordered_map<ConnectionType, ConnectionPool> pools;
        std::unordered_map<ConnectionType, std::vector<Buffer*>> queues;
        Config config;

};