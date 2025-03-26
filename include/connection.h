#pragma once

#include <cstdint>
#include <memory>
#include <netinet/in.h>
#include <nghttp2/nghttp2.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unordered_map>
#include <string>
#include "buffer.h"
#include "connection_enums.h"
#include "rpc_mapper.h"
#include "rpc_queue.h"



class NoConnectionException : public std::runtime_error {
    public:
        NoConnectionException() : std::runtime_error("No connection available") {}
};

typedef struct CallbackData {
    ConnectionType type;
    ConnectionDirection direction;
    int fd;
    RPCQueue* queue;
    RPCMapper* mapper;
} CallbackData;

class HTTPConnection {

    public:
        /**
         * @brief Construct an upstream connection
         * @note This is used by state
         */
        HTTPConnection(std::string, int, ConnectionType, RPCQueue*, RPCMapper*);

        /*
         * @brief Construct an downstream connection
         * @note This is used by listeners
         */
        HTTPConnection(int, ConnectionType, RPCMapper*, RPCQueue*); 
        ~HTTPConnection();
        int get_fd() { return fd; }
        sockaddr* get_addr();
        std::string type_to_str();
        std::string direction_to_str();
        ConnectionStatus get_status() { return status; }
        void set_status(ConnectionStatus s) { status = s; }
        void http_read(Buffer*);
        bool want_write();
        int http_write(Buffer*);
        void submit_settings();
        int32_t submit_request(RPCMessage&);
        void submit_response(RPCMessage&);

    private:
        int fd; // local socket file descriptor
        sockaddr_in addr;
        ConnectionStatus status;
        nghttp2_session* session;
        nghttp2_session_callbacks* callbacks;
        std::unique_ptr<CallbackData> callback_data;

    public:
        ConnectionType type;
        ConnectionDirection direction;
    
    private:
        static void set_callbacks(nghttp2_session_callbacks*);

};

class ConnectionPool {

    public:
    ConnectionPool(ConnectionType);

        /**
         * @brief Add a connection to the pool
         */
        std::unique_ptr<HTTPConnection>& add_connection(std::string&, int, RPCMapper*, RPCQueue*);
        std::unique_ptr<HTTPConnection>& get_connection(int fd) { return connections[fd]; }
        std::unique_ptr<HTTPConnection>& get_any_connection();
        bool has_connection(int fd);
        void remove_connection(int fd) { connections.erase(fd); }
    
    private:
        std::unordered_map<int, std::unique_ptr<HTTPConnection>> connections; // fd: connection
        ConnectionType type;
};