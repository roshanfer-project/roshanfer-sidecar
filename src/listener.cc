#include "connection_enums.hpp"
#include "stats.hpp"
#include <arpa/inet.h>
#include <connection.hpp>
#include <glog/logging.h>
#include <listener.hpp>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>

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
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
    LOG(FATAL) << "setsockopt(SO_REUSEADDR) failed";
  }

  // set SO_REUSEPORT to allow multiple threads to bind to the same port
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(enable)) < 0) {
    LOG(FATAL) << "setsockopt(SO_REUSEPORT) failed";
  }

  if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
    LOG(FATAL) << "Failed to bind socket: " << port;
  }

  // Listen on the socket
  /*
  NOTEs:
  - If backlog of connections (here is 100) is small, you will seeInvalid
  Argument errors in cqe of io_uring_prep_accept
  - Small backlog size can also cause "Connection timed out" in Ingress when the
  connection pool size is larger than it. In this case, Frontend's sidecar drops
  CONNECT requests from Ingress.
  */
  if (listen(fd, 300) < 0) {
    LOG(FATAL) << "Failed to listen on socket: " << port;
  }

  VLOG(1) << "Listener created on port: " << port << " with fd: " << fd;
};

Listener::~Listener() {
  LOG(FATAL) << "Listener deconstructor on port: " << port
             << " with fd: " << fd;
}

std::shared_ptr<HTTPConnection>
Listener::add_connection(int new_fd, RPCMapper *mapper, RPCQueue *queue,
                         HTTP http, Stats *stats) {
  if (!mapper || !queue || !stats) {
    LOG(FATAL) << "Null pointer parameters: mapper=" << mapper
               << ", queue=" << queue << ", stats=" << stats;
  }
  try {
    if (http == HTTP::HTTP1) {
      connections.emplace(new_fd, std::make_shared<HTTP1Connection>(
                                      new_fd, type, mapper, queue, stats));
    } else {
      connections.emplace(new_fd, std::make_shared<HTTP2Connection>(
                                      new_fd, type, mapper, queue, stats));
    }
    return connections.at(new_fd);
  } catch (const std::exception &e) {
    LOG(FATAL) << "Failed to add connection: " << e.what() << ", fd: " << new_fd
               << ", type: " << type_to_str();
  }
};

std::shared_ptr<HTTPConnection> Listener::get_connection(int search_fd) {
  try {
    return connections.at(search_fd);
  } catch (const std::out_of_range &e) {
    // print all connections
    LOG(INFO) << "Current connections:";
    for (const auto &[it_fd, conn] : connections) {
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

void Listener::dump_connections() {
  LOG(INFO) << "Dumping connections for listener on port: " << port;
  LOG(INFO) << "Type: " << type_to_str();
  for (const auto &[it_fd, conn] : connections) {
    LOG(INFO) << "  fd: " << it_fd << ", type: " << conn->type_to_str()
              << ", direction: " << conn->direction_to_str();
  }
}
