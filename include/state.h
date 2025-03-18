#pragma once

#include <cstdint>
#include <memory>
#include <sys/types.h>
#include <vector>
#include "config.h"
#include "connection.h"
#include "buffer_manager.h"
#include "ring_wrapper.h"
#include "stats.h"
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

class PPMState  {
    public:
        PPMState();
    
    public:
        int sent_credits;
        int sent_dns;
        int received_dns;
};

class State {

    public:
        State(Config, RingWrapper&, BufferManager&);
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

        // PPM-related functions
        void update_state(HTTPConnection&);
        void queue_multiplexer(Buffer*, Buffer*);
        void ppm_client(bool, Buffer*);

    private:
        void udp_send(std::span<char>, std::string&, uint16_t);

        // PPM-related functions
        void send_dn();
        bool valid_credit(const char*);
        void send_from_ppm_queue();


    private:
        std::unordered_map<ConnectionType, ConnectionPool> pools;
        std::unordered_map<ConnectionType, std::vector<Buffer*>> queues;
        Config config;
        RingWrapper& ring;
        BufferManager& buffer_manager;
        int sockfd;
    
    public:
        std::vector<Buffer*> ppm_queue;
        Stats stats;
        PPMState ppm_state;

};