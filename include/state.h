#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <sys/types.h>
#include "config.h"
#include "connection.h"
#include "buffer_manager.h"
#include "connection_enums.h"
#include "ingress.h"
#include "ppm_queue.h"
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
        std::unordered_map<std::string, uint8_t, TransparentHash, TransparentEqual> denied_reqs;
};

class UpstreamRouteMapper{
    public:
        UpstreamRouteMapper();
        void add_route(std::string);
        ConnectionPool& get_pool(const std::string&);
    
    private:
        std::unordered_map<std::string, ConnectionPool> map;
};

class State {

    public:
        State(Config, RingWrapper&, BufferManager&, RPCMapper&, RPCQueue&,
            std::unordered_map<ConnectionType, Listener>&, Ingress&);
        void forward(ConnectionType, ConnectionDirection);
        void remove_connection(HTTPConnection&);

        // PPM-related functions
        void queue_multiplexer(Buffer*, Buffer*);
        void ppm_client(bool, Buffer*);
        void ingress_admit();
        
        void write_http(HTTPConnection*);
        bool forward_request(HTTPConnection*, RPCMessage*);
        HTTPConnection* route_request(ConnectionType, uint32_t, int);
    private:
        void udp_send(std::span<char>, struct sockaddr_in*);

        // PPM-related functions
        void send_dn(HTTPConnection*, const std::string&);
        std::pair<const std::string&, bool> valid_credit(const char*);
        void send_from_ppm_queue();


    private:
        ConnectionPool ingress_pool;
        UpstreamRouteMapper upstream_route_mapper;
        Config config;
        RingWrapper& ring;
        BufferManager& buffer_manager;
        int sockfd; // UDP socket file descriptor
        RPCMapper& rpc_mapper;
        RPCQueue& rpc_queue;
        std::unordered_map<ConnectionType, Listener>& listeners;
        PPMQueue ppm_queue;
        Ingress& ingress;
    
    public:
        Stats stats;
        PPMState ppm_state;

};