#pragma once

#include <cstdint>
#include <memory>
#include <sys/types.h>
#include "config.h"
#include "connection.h"
#include "buffer_manager.h"
#include "connection_enums.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <unordered_map>
#include <span>

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

class PPMState  {
    public:
        PPMState();
    
    public:
        int sent_credits;
        int sent_dns;
        int received_dns;
        int received_credits;
        int unused_credits;
};

class State {

    public:
        State(Config, RingWrapper&, BufferManager&, RPCMapper&, RPCQueue&,
            std::unordered_map<ConnectionType, Listener>&);
        void route(ConnectionType, ConnectionDirection);
        std::unique_ptr<HTTPConnection>& get_connection(int, ConnectionType);
        void remove_connection(int, ConnectionType);
        void remove_one_connection(ConnectionType);
        HTTPConnection& get_one_connection(ConnectionType);

        // PPM-related functions
        void queue_multiplexer(Buffer*, Buffer*);
        void ppm_client(bool, Buffer*);
        void write_http(HTTPConnection*);

        bool route_request(uint32_t, ConnectionType);
    private:
        void udp_send(std::span<char>, std::string&, uint16_t);

        void report_latency(RPCMessage&, ConnectionType);

        // PPM-related functions
        void send_dn();
        bool valid_credit(const char*);
        void send_from_ppm_queue();


    private:
        std::unordered_map<ConnectionType, ConnectionPool> pools;
        Config config;
        RingWrapper& ring;
        BufferManager& buffer_manager;
        int sockfd; // UDP socket file descriptor
        RPCMapper& rpc_mapper;
        RPCQueue& rpc_queue;
        std::unordered_map<ConnectionType, Listener>& listeners;
    
    public:
        Stats stats;
        PPMState ppm_state;

};