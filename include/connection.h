#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <netinet/in.h>
#include <nghttp2/nghttp2.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unordered_map>
#include <string>
#include <stdexcept>
#include "buffer.h"
#include "connection_enums.h"
#include "ingress.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"



class NoConnectionException : public std::runtime_error {
    public:
        NoConnectionException(std::string msg) : std::runtime_error(msg) {}
};

typedef struct CallbackData {
    ConnectionType type;
    ConnectionDirection direction;
    int fd;
    RPCQueue* queue;
    RPCMapper* mapper;
    ConnectionStatus* status;
    struct hdr_histogram* hist;
} CallbackData;

class HTTPConnection {

    public:
        /**
         * @brief Construct an upstream connection
         * @note This is used by state
         */
        HTTPConnection(std::string, uint16_t, ConnectionType, struct hdr_histogram*);

        /*
         * @brief Construct an downstream connection
         * @note This is used by listeners
         */
        HTTPConnection(int, ConnectionType, struct hdr_histogram*);
        virtual ~HTTPConnection() = default;
        int get_fd() { return fd; }
        sockaddr* get_addr();
        std::string type_to_str();
        std::string direction_to_str();
        ConnectionStatus get_status() { return status; }
        void set_status(ConnectionStatus s) { status = s; }
        uint16_t get_port() { return port; }
        std::string& get_host() { return host; }

        // pure virtual functions
        virtual void http_read(Buffer*, Ingress&) = 0;
        virtual bool want_write() = 0;
        virtual int http_write(Buffer*) = 0;
        virtual void submit_settings() = 0;
        virtual int32_t submit_request(RPCMessage&) = 0;
        virtual void submit_response(RPCMessage&) = 0;
        virtual void submit_error_response(RPCMessage&) = 0;
        virtual bool available() = 0;
        virtual HTTP http() = 0;
        
    protected:
        int fd; // local socket file descriptor
        sockaddr_in addr;
        ConnectionStatus status;
        std::string host;
        uint16_t port;
        struct hdr_histogram* hist;

    public:
        ConnectionType type;
        ConnectionDirection direction;
};


class HTTP2Connection : public HTTPConnection {

    public:
        HTTP2Connection(std::string, uint16_t, ConnectionType, RPCQueue*, RPCMapper*, struct hdr_histogram*);
        HTTP2Connection(int, ConnectionType, RPCMapper*, RPCQueue*, struct hdr_histogram*);
        ~HTTP2Connection();

        void http_read(Buffer*, Ingress&);
        bool want_write();
        int http_write(Buffer*);
        void submit_settings();
        int32_t submit_request(RPCMessage&);
        void submit_response(RPCMessage&);
        void submit_error_response(RPCMessage&);
        bool available() { return true;}
        HTTP http() { return HTTP::HTTP2; }

    private:
        nghttp2_session* session;
        nghttp2_session_callbacks* callbacks;
        std::unique_ptr<CallbackData> callback_data;
    
    private:
        static void set_callbacks(nghttp2_session_callbacks*);

};


const size_t HTTP1Connection_BUF_SIZE = 200000;
const size_t HTTP1Connection_MAX_HEADERS = 10;


class HTTP1Connection : public HTTPConnection {

    public:
        HTTP1Connection(std::string, uint16_t, RPCMapper*, RPCQueue*, struct hdr_histogram*);
        HTTP1Connection(int, RPCMapper*, RPCQueue*, struct hdr_histogram*);
        ~HTTP1Connection();

        void http_read(Buffer*, Ingress&);
        bool want_write();
        int http_write(Buffer*);
        void submit_settings();
        int32_t submit_request(RPCMessage&);
        void submit_response(RPCMessage&);
        void submit_error_response(RPCMessage&);
        bool available();
        HTTP http() { return HTTP::HTTP1; }
    
    private:
        void set_rpc_message(HTTPMessage* msg);
        HTTPMessage* get_rpc_message();
        int parse_http1_request(Buffer*);
        
    private:
        // internal state for parsing
        std::array<char, HTTP1Connection_BUF_SIZE> buf;
        size_t buf_len;
        size_t prev_buf_len;
        bool hdr_complete;
        int content_length;
        int hdr_size;


        bool idle;
        RPCMapper* mapper;
        RPCQueue* queue;
        int32_t last_id;
        HTTPMessage* rpc_message;
};

class ConnectionPool {

    public:
    ConnectionPool(ConnectionType);

        /**
         * @brief Add a connection to the pool
         */
        std::unique_ptr<HTTPConnection>& add_connection(const std::string&, int, RPCMapper*, RPCQueue*, HTTP,
             struct hdr_histogram*);
        std::unique_ptr<HTTPConnection>& get_connection(int fd) { return connections.at(fd); }
        std::unique_ptr<HTTPConnection>& get_any_connection();
        bool has_connection(int fd);
        void remove_connection(int fd) { connections.erase(fd); }
    
    private:
        std::unordered_map<int, std::unique_ptr<HTTPConnection>> connections; // fd: connection
        ConnectionType type;
};