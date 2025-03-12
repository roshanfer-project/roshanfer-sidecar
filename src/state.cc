#include "state.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "glog/logging.h"
#include <memory>
#include <vector>

State::State(Config config)
:   config(config),
    queues(),
    pools()
    {
        pools.emplace(ConnectionType::EGRESS, ConnectionPool(ConnectionType::EGRESS));
        pools.emplace(ConnectionType::INGRESS, ConnectionPool(ConnectionType::INGRESS));
        queues.emplace(ConnectionType::EGRESS, std::vector<Buffer*>());
        queues.emplace(ConnectionType::INGRESS, std::vector<Buffer*>());
}


int State::route(ConnectionType type) {
    try {
        auto& conn = pools.at(type).get_any_connection();
        if (conn->get_status() == ConnectionStatus::DOWN) {
            throw ConnectionNotUPException(conn);
        }
        return conn->get_fd();
    } catch (NoConnectionException& e) {
        LOG(INFO) << "No connection available. Starting a new connection.";

        std::string host;
        int port;
        if (type == ConnectionType::EGRESS) {
            host = config.endpoint_host;
            port = config.endpoint_port;
        } else if (type == ConnectionType::INGRESS) {
            host = config.ingress_upstream_host;
            port = config.ingress_upstream_port;
        } else {
            LOG(FATAL) << "Unknown connection type";
        }
        std::unique_ptr<HTTPConnection>& conn = pools.at(type).add_connection(
            host, port);
        DLOG(INFO) << "New connection established on fd: " << conn->get_fd();
        throw AddConnectionException(conn);
    }
}

void State::add_buffer(Buffer* buffer, ConnectionType type) {
    queues[type].push_back(buffer);
}

bool State::has_buffer(ConnectionType type) {
    return !queues[type].empty();
}

Buffer* State::get_buffer(ConnectionType type) {
    Buffer* buffer;
    buffer = queues[type].front();
    queues[type].erase(queues[type].begin());
    return buffer;
}

std::unique_ptr<HTTPConnection>& State::get_connection(int fd, ConnectionType type) {
    return  pools.at(type).get_connection(fd);
}

void State::remove_connection(int fd, ConnectionType type) {
    pools.at(type).remove_connection(fd);
}
