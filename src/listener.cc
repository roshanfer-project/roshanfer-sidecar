#include <listener.h>
#include <connection.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <memory>
#include <glog/logging.h>




Listener::Listener(uint16_t port, ConnectionType type) 
    : port(port), type(type) {
    
    // Create a socket
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    // Bind the socket
    struct sockaddr_in addr;
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

HTTPConnection& Listener::add_connection(int fd, RPCMapper* mapper, RPCQueue* queue, bool is_http1, 
                                         struct hdr_histogram* hist) {
    if (is_http1) {
        connections[fd] = std::make_unique<HTTP1Connection>(fd, mapper, queue, hist);
    } else {
        connections[fd] = std::make_unique<HTTP2Connection>(fd, type, mapper, queue, hist);
    }
    return *connections[fd];
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

