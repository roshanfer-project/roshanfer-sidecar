#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <sys/types.h>
#include "config.h"
#include "connection.h"
#include "buffer_manager.h"
#include "connection_enums.h"
#include "hdr/hdr_histogram.h"
#include "ingress.h"
#include "ppm_queue.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <unordered_map>

class AddConnectionException : public std::runtime_error {
    public:
        AddConnectionException(std::unique_ptr<HTTPConnection>& ex_conn)
         : std::runtime_error(""), conn(ex_conn) {
        }
    
        std::unique_ptr<HTTPConnection>& conn;
};

class ConnectionNotUPException: public std::runtime_error {
    public:
        ConnectionNotUPException(std::unique_ptr<HTTPConnection>& ex_conn) 
            : std::runtime_error(""), conn(ex_conn) {}


        std::unique_ptr<HTTPConnection>& conn;
};

class PPMState  {
    public:
        PPMState();
    
    public:
        // hosted services and their sent credits
        std::unordered_map<std::string, uint32_t, TransparentHash, TransparentEqual> sent_credits;
        std::unordered_map<std::string, uint32_t, TransparentHash, TransparentEqual> per_method_resp_in;
        std::unordered_map<std::string, uint32_t> downstream_conccurency;
        // downstream services and their denied requests
        std::unordered_map<std::string, uint32_t, TransparentHash, TransparentEqual> denied_reqs;
        std::unordered_map<std::string, uint32_t, TransparentHash, TransparentEqual> ingress_admitted;
        std::unordered_map<std::string, uint32_t> ingress_transmitted;
        std::unordered_map<std::string, bool> ppm_client_dn_send;
        std::unordered_map<std::string, uint32_t> new_ppm_queue_reqs;
        std::unordered_map<std::string, uint32_t> local_concurrency_limit;
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
        struct hdr_histogram* get_histogram() { return hist; }
        
        /*Write request/response from connection's internal state to buffers. 
        For HTTP/2 it also writes setting/ping/etc frames.*/
        void write_http(HTTPConnection*);
        bool forward_request(HTTPConnection*, RPCMessage*);
        HTTPConnection* route_request(ConnectionType, int32_t, int);
        void dump_entire_state();
    private:
        void udp_send(std::vector<char>, struct sockaddr_in*);

        // PPM-related functions
        void send_dn(HTTPConnection*, const std::string&, size_t);
        std::tuple<const std::string&, bool, size_t> valid_credit(const char*);
        void send_from_ppm_queue();


    private:
        Config config;
        ConnectionPool ingress_pool;
        UpstreamRouteMapper upstream_route_mapper;
        RingWrapper& ring;
        BufferManager& buffer_manager;
        int sockfd; // UDP socket file descriptor
        RPCMapper& rpc_mapper;
        RPCQueue& rpc_queue;
        std::unordered_map<ConnectionType, Listener>& listeners;
        PPMQueue ppm_queue;
        Ingress& ingress;
        struct hdr_histogram* hist;
        std::chrono::steady_clock::time_point next_hist_update;

    
    public:
        Stats stats;
        PPMState ppm_state;

};