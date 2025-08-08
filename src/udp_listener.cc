#include "udp_listener.h"
#include "glog/logging.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>

UDPListner::UDPListner(uint16_t port) {
    // create a socket
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    // set SO_REUSEPORT to allow multiple threads to bind to the same port
    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        LOG(FATAL) << "Failed to set SO_REUSEPORT: " << strerror(errno);
    }

    // bind the socket
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    int ret = bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (ret < 0) {
        LOG(FATAL) << "Failed to bind socket, error: "
                   << strerror(errno);
    }
}