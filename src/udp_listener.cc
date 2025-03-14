#include "udp_listener.h"
#include "glog/logging.h"
#include <sys/socket.h>
#include <netinet/in.h>

UDPListner::UDPListner(uint16_t port) {
    // create a socket
    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    // bind the socket
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        LOG(FATAL) << "Failed to bind socket";
    }
}