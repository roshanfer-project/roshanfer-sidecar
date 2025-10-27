#include "connection_enums.h"
#include <listener.h>
#include <connection.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <memory>
#include <glog/logging.h>




Listener::Listener(uint16_t lis_port, ConnectionType lis_type) 
    : port(lis_port), addr({}), type(lis_type) {
    
    // Create a socket
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    // Bind the socket
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    int enable = 1;
    if (setsockopt(fd,
        SOL_SOCKET, SO_REUSEADDR,
        &enable, sizeof(int)) < 0) {
            LOG(FATAL) << "setsockopt(SO_REUSEADDR) failed";
    }
    
    // set SO_REUSEPORT to allow multiple threads to bind to the same port
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(enable)) < 0) {
        LOG(FATAL) << "setsockopt(SO_REUSEPORT) failed";
    }
    
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG(FATAL) << "Failed to bind socket: " << port;
    }

    // Listen on the socket
    // NOTE: if backlog of connections (here is 100) is small, you will see Invalid Argument 
    // errors in cqe of io_uring_prep_accept
    if (listen(fd, 100) < 0) {
        LOG(FATAL) << "Failed to listen on socket: " << port;
    }

    VLOG(1) << "Listener created on port: " << port << " with fd: " << fd;
};

Listener::~Listener() {
    LOG(FATAL) << "Listener deconstructor on port: " << port << " with fd: " << fd;
}

std::shared_ptr<HTTPConnection> Listener::add_connection(int new_fd, RPCMapper* mapper, RPCQueue* queue, HTTP http, 
                                         struct hdr_histogram* hist) {
    if (!mapper || !queue || !hist) {
        LOG(FATAL) << "Null pointer parameters: mapper=" << mapper << ", queue=" << queue << ", hist=" << hist;
    }
    try {
        if (http == HTTP::HTTP1) {
            connections.emplace(new_fd, std::make_shared<HTTP1Connection>(new_fd, type, mapper, queue, hist));
        } else {
            connections.emplace(new_fd, std::make_shared<HTTP2Connection>(new_fd, type, mapper, queue, hist));
        }
        return connections.at(new_fd);
    } catch (const std::exception& e) {
        LOG(FATAL) << "Failed to add connection: " << e.what()
                   << ", fd: " << new_fd
                   << ", type: " << type_to_str();
    }
};

std::shared_ptr<HTTPConnection> Listener::get_connection(int search_fd) {
    try {
        return connections.at(search_fd);
    } catch (const std::out_of_range& e) {
        // print all connections
        LOG(INFO) << "Current connections:";
        for (const auto& [it_fd, conn] : connections) {
            LOG(INFO) << "  fd: " << it_fd << ", type: " << conn->type_to_str()
                      << ", direction: " << conn->direction_to_str();
        }
        // print the error
        LOG(FATAL) << "Connection not found for fd: " << search_fd;
    }
};

std::string Listener::type_to_str() {
    if (type == ConnectionType::INGRESS) {
        return "INGRESS";
    } else if (type == ConnectionType::EGRESS) {
        return "EGRESS";
    } else {
        LOG(FATAL) << "Unknown connection type";
    }
};

