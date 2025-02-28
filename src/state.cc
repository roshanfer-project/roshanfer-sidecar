#include "state.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "glog/logging.h"
#include <memory>
#include <vector>

State::State(Config config)
    : pool(std::make_unique<ConnectionPool>()),
      config(config),
      queue(std::vector<Buffer*>()) {}


int State::route(ConnectionType type) {
    if (type == ConnectionType::EGRESS) {
    // TODO: this just returns the first connection
    try {
        return pool->get_any_connection()->get_fd();
    } catch (NoConnectionException& e) {
        LOG(INFO) << "No connection available. Starting a new connection.";
        std::unique_ptr<TCPConnection>& conn = pool->add_connection("127.0.0.1", config.endpoint_port);
        DLOG(INFO) << "New connection established on fd: " << conn->get_fd();
        throw AddConnectionException(conn);
    }
    } else if (type == ConnectionType::INGRESS) {
        LOG(FATAL) << "Ingress routing not implemented";
    } else {
        LOG(FATAL) << "Unknown connection type";
    }
}

void State::add_buffer(Buffer* buffer) {
    queue.push_back(buffer);
}

bool State::has_buffer() {
    return !queue.empty();
}

Buffer* State::get_buffer() {
    Buffer* buffer = queue.front();
    queue.erase(queue.begin());
    return buffer;
}

