#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
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
#include "spinlock.hpp"
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

class FailedDNInfoUnit {
    public:
        FailedDNInfoUnit(struct sockaddr_in, int, int16_t);

        // delete copy semantics
        FailedDNInfoUnit(const FailedDNInfoUnit&) = delete;
        FailedDNInfoUnit& operator=(const FailedDNInfoUnit&) = delete;

        // delete move semantics
        FailedDNInfoUnit(FailedDNInfoUnit&&) = delete;
        FailedDNInfoUnit& operator=(FailedDNInfoUnit&&) = delete;
    public:
        std::unique_ptr<struct sockaddr_in> addr;
        int num_rejected_requests;
        int16_t id;
};

class FailedDNInfo {
    public:
        FailedDNInfo();

        // delete copy semantics
        FailedDNInfo(const FailedDNInfo&) = delete;
        FailedDNInfo& operator=(const FailedDNInfo&) = delete;

        // delete move semantics
        FailedDNInfo(FailedDNInfo&&) = delete;
        FailedDNInfo& operator=(FailedDNInfo&&) = delete;

        void push(std::unique_ptr<FailedDNInfoUnit>);
        std::unique_ptr<FailedDNInfoUnit> pop();
        std::string id_list();
        size_t size() { return failed_dn_info.size(); }

    public:
        std::deque<std::unique_ptr<FailedDNInfoUnit>> failed_dn_info;
};

class SharedState  {
    public:
        SharedState(std::vector<std::string>, std::vector<std::string>);
    
    public:
        /*ConnectionType::INGRESS-side metrics*/

        /*
        hosted services and their sent credits
        Note that this is shared among threads for the same reason as ingress_admitted.
        */
        FastMap<uint32_t> sent_credits;
        /*
        per-method ConnectionType::INGRESS responses in to the sidecar (from the local app). In other words,
        this is final response for a service method.
        Note that Ingress::Ingress does not use this because it relies on ConnectionType::EGRESS responses.
        This is shared among threads for the same reason as ingress_admitted.
        */
        FastMap<uint32_t> per_method_resp_in;
        /*
        This counts the number of ConnectionType::INGRESS requests admitted to the sidecar
        Note that Ingress::Ingress does not use this.
        The reason that this is shared among threads is because we are relying on the kernel
        to balance connections between threads. Therefore, we might have different threads 
        receiving requests for the same service (For frontend, this is defenitely the case now).
        */
        FastMap<uint32_t> ingress_request_admitted;

        LocalMap<FailedDNInfo> failed_dn_info;


        /*ConnectionType::EGRESS-side metrics*/

        /*
        downstream services and their concurrency
        Note that this is shared among threads for the same reason as ingress_admitted.
        */
        FastMap<int64_t> downstream_concurrency;
};

class LocalState {
    public:
        LocalState(std::vector<std::string>, std::vector<std::string>);
    
    public:
        /*ConnectionType::INGRESS-side metrics*/

        /*
        READ-ONLY: Global concurrency limit for all services (config.ppm_limit).
        */
        LocalMap<uint32_t> local_concurrency_limit;
        /*
        READ-ONLY: per-API limit (config.mapping.<service>.limit).
        */
        LocalMap<uint32_t> per_api_limit;


        /*ConnectionType::EGRESS-side metrics*/

        // True for downstream services if we have received a response form them or
        // there is a new request in the PPM queue for that service
        std::unordered_map<std::string, bool> ppm_client_dn_send;
        // Number of new requests in the PPM queue for each service
        LocalMap<uint32_t> new_ppm_queue_reqs;
        /*
        READ-ONLY: This is a mapping from downstream services to upstream services.
        */
        LocalMap<std::string> upstream_service;
        /*
        Number of received responses for each service .
        This is EGRESS equivalent of `per_method_resp_in`.
        */
        LocalMap<uint32_t> egress_resp_in;
        // number of drops (updated if only config.is_ingress is true)
        uint32_t drops;
        /*
        ONLY used by Ingress::Ingress to track the number of requests admitted to RPCQueue for admittion.
        */
        LocalMap<int64_t> ingress_to_be_admitted;
        /*
        ONLY used by Ingress::Ingress to track the number of requests admitted to frontend.
        */
        LocalMap<int64_t> ingress_admitted;
        /*
        READ-ONLY: ONLY used by Ingress::Ingress to track the ingress limit.
        */
        LocalMap<int32_t> ingress_limit;

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
            std::unordered_map<ConnectionType, std::shared_ptr<Listener>>&, Ingress&, SharedState&, std::string&, int);
        void forward(ConnectionType, ConnectionDirection);
        void remove_connection(std::shared_ptr<HTTPConnection>);

        // PPM-related functions
        void queue_multiplexer(const std::unique_ptr<Buffer>&, const std::unique_ptr<Buffer>&);
        void ppm_client(bool, const std::unique_ptr<Buffer>&);
        void ingress_admit();
        struct hdr_histogram* get_histogram() { return hist; }
        
        /*Write request/response from connection's internal state to buffers. 
        For HTTP/2 it also writes setting/ping/etc frames.*/
        void write_http(std::shared_ptr<HTTPConnection>);
        bool forward_request(std::shared_ptr<HTTPConnection>, std::shared_ptr<RPCMessage>);
        std::shared_ptr<HTTPConnection> route_request(ConnectionType, int32_t, int);
        void dump_entire_state();
        int get_sockfd() { return sockfd; }
    private:
        void udp_send(std::vector<char>, struct sockaddr_in*);

        // PPM-related functions
        void send_dn(HTTPConnection*, const std::string&, size_t, int16_t);
        std::tuple<const std::string&, bool, size_t, int16_t> valid_credit(const char*);
        int get_available_credits(const std::string_view&);
        void check_credit_transmission(int16_t);
        void send_credit(std::unique_ptr<struct sockaddr_in>&, const std::string_view&, int, int16_t);


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
        struct hdr_histogram* hist;
        std::chrono::steady_clock::time_point next_hist_update;
        int thread_id;
        SpinLock failed_dn_info_lock;
    
    public:
        SharedState& shared_state;
        LocalState local_state;
        Utilization utilization;
        std::string& ingress_service;
        Stats stats;
};