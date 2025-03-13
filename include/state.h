#pragma once

#include <cstdint>
#include <memory>
#include <sys/types.h>
#include <vector>
#include "config.h"
#include "connection.h"
#include "buffer_manager.h"
#include "http2_parser.h"
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

enum class GRPCMESSAGE {
    REQUEST, RESPONSE
};

class RPCMessage {

    public:
        RPCMessage(GRPCMESSAGE);
        bool add_frame(HTTP2Frame&);

    private:
        std::vector<HTTP2Frame> frames;
        GRPCMESSAGE type;
};

class Metrics {
    public:
        Metrics();
        void add_resp_out(uint8_t);
        uint8_t get_resp_out();

    private:
        uint8_t resp_out;
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
        void remove_one_connection(ConnectionType);
        HTTPConnection& get_one_connection(ConnectionType);
        /**
        based on the connection, update stats, buffer incomplete messages.
        PPM client logic will be here and the Buffer can be modified.
        */
        void update_state(HTTPConnection&, Buffer*);

    private:
        std::tuple<std::unordered_map<uint32_t, RPCMessage>, std::vector<HTTP2Frame>>
            analyze_messages(std::vector<HTTP2Frame>&, bool);


    private:
        std::unordered_map<ConnectionType, ConnectionPool> pools;
        std::unordered_map<ConnectionType, std::vector<Buffer*>> queues;
        Config config;
    
    public:
        Metrics metrics;

};