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
#include "fast_map.hpp"
#include "hdr/hdr_histogram.h"
#include "ingress.h"
#include "ppm_queue.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <unordered_map>
#include <vector>

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

class SharedState  {
    public:
        SharedState(std::vector<std::string>, std::vector<std::string>);
    
    public:
        /*Ingress-side metrics*/
        // hosted services and their sent credits
        FastMap<uint32_t> sent_credits;
        // per-method ingress responses in to the sidecar (from the local app)
        FastMap<uint32_t> per_method_resp_in;
        // ingress requests admitted to the sidecar
        FastMap<uint32_t> ingress_admitted;

        /*Egress-side metrics*/
        // downstream services and their concurrency
        FastMap<int64_t> downstream_concurrency;
        // Only if config.is_ingress is true, number of transmitted requests by ppm client
        FastMap<uint32_t> ingress_transmitted;
};

class LocalState {
    public:
        LocalState(std::vector<std::string>, std::vector<std::string>);
    
    public:
        /*Ingress-side metrics*/
        // Local concurrency limit for each service
        LocalMap<uint32_t> local_concurrency_limit;
        // per-API limit
        LocalMap<uint32_t> per_api_limit;

        /*Egress-side metrics*/
        // downstream services and their denied requests
        LocalMap<uint32_t> denied_reqs;
        // True for downstream services if we have received a response form them or
        // there is a new request in the PPM queue for that service
        std::unordered_map<std::string, bool> ppm_client_dn_send;
        // Number of new requests in the PPM queue for each service
        LocalMap<uint32_t> new_ppm_queue_reqs;
        // Number of received responses for each service
        LocalMap<uint32_t> egress_resp_in;
        // number of drops (updated if only config.is_ingress is true)
        uint32_t drops;
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
            std::unordered_map<ConnectionType, std::shared_ptr<Listener>>&, Ingress&, SharedState&, std::string&);
        void forward(ConnectionType, ConnectionDirection);
        void remove_connection(std::shared_ptr<HTTPConnection>);

        // PPM-related functions
        void queue_multiplexer(const std::unique_ptr<Buffer>&, const std::unique_ptr<Buffer>&);
        void ppm_client(bool, const std::unique_ptr<Buffer>&);
        void ingress_admit();
        std::shared_ptr<struct hdr_histogram> get_histogram() { return hist; }
        
        /*Write request/response from connection's internal state to buffers. 
        For HTTP/2 it also writes setting/ping/etc frames.*/
        void write_http(std::shared_ptr<HTTPConnection>);
        bool forward_request(std::shared_ptr<HTTPConnection>, std::shared_ptr<RPCMessage>);
        std::shared_ptr<HTTPConnection> route_request(ConnectionType, int32_t, int);
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
        std::unordered_map<ConnectionType, std::shared_ptr<Listener>>& listeners;
        PPMQueue ppm_queue;
        Ingress& ingress;
        std::shared_ptr<struct hdr_histogram> hist;
        std::chrono::steady_clock::time_point next_hist_update;

    
    public:
        SharedState& shared_state;
        LocalState local_state;
        Utilization utilization;
        std::string& ingress_service;
};