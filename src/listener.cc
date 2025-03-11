#include <listener.h>
#include <connection.h>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <memory>
#include <glog/logging.h>




Listener::Listener(uint16_t port, ConnectionType type) 
    : port(port), type(type) {
    
    if (type == ConnectionType::EGRESS) {
    
        // Create a socket
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            throw std::runtime_error("Failed to create socket");
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
            throw std::runtime_error("Failed to bind socket");
        }

        // Listen on the socket
        if (listen(fd, 10) < 0) {
            throw std::runtime_error("Failed to listen on socket");
        }
    } else if (type == ConnectionType::INGRESS) {
        LOG(FATAL) << "Ingress listener not implemented";
    }

    DLOG(INFO) << "Listener created on port: " << port << " with fd: " << fd;
};

HTTPConnection& Listener::add_connection(int fd) {
    connections[fd] = std::make_unique<HTTPConnection>(fd);
    return *connections[fd];
};



