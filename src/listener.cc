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

HTTPConnection& Listener::add_connection(int new_fd, RPCMapper* mapper, RPCQueue* queue, HTTP http, 
                                         struct hdr_histogram* hist) {
    if (http == HTTP::HTTP1) {
        connections.emplace(new_fd, std::make_unique<HTTP1Connection>(new_fd, mapper, queue, hist));
    } else {
        connections.emplace(new_fd, std::make_unique<HTTP2Connection>(new_fd, type, mapper, queue, hist));
    }
    return *connections.at(new_fd);
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

