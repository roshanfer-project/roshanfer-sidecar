#include "state.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "glog/logging.h"
#include "ring_wrapper.h"
#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <unordered_map>
#include <vector>

State::State(Config config, RingWrapper& ring, BufferManager& buffer_manager)
:   config(config),
    queues(),
    pools(),
    stats(),
    ppm_state(),
    buffer_manager(buffer_manager),
    ring(ring) {
        pools.emplace(ConnectionType::EGRESS, ConnectionPool(ConnectionType::EGRESS));
        pools.emplace(ConnectionType::INGRESS, ConnectionPool(ConnectionType::INGRESS));
        queues.emplace(ConnectionType::EGRESS, std::vector<Buffer*>());
        queues.emplace(ConnectionType::INGRESS, std::vector<Buffer*>());

        sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd < 0) {
            LOG(FATAL) << "Failed to create socket";
        }
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

void State::remove_one_connection(ConnectionType type) {
    pools.at(type).remove_connection(pools.at(type).get_any_connection()->get_fd());
}

HTTPConnection& State::get_one_connection(ConnectionType type) {
    return *pools.at(type).get_any_connection().get();
}

void State::remove_connection(int fd, ConnectionType type) {
    pools.at(type).remove_connection(fd);
}

void State::update_state(HTTPConnection& conn) {
    ppm_client(false, nullptr);
}

bool State::valid_credit(const char* data) {
    if (data[0] != 0x01) {
        LOG(FATAL) << "Invalid message type";
    }

    if (data[1] != 0x01) {
        LOG(FATAL) << "Expected a response";
    }

    ppm_state.received_dns++;

    return data[2] == 0x01;
}


void State::send_from_ppm_queue() {
    if (ppm_queue.size() > 0) {
        DLOG(INFO) << "Send a request from PPM queue";

        // send one queued request (from the end of queue)
        auto& msg = ppm_queue.back();

        // send the request using io-uring
        // Note that we don't need to do any routing here
        auto& conn = pools.at(ConnectionType::EGRESS).get_any_connection();
        msg->get_buffer()->prepare_write(conn.get());
        auto ud = buffer_manager.get_user_data();
        ud->rpc_message = std::move(msg);
        ppm_queue.pop_back();

        ring.prepare_write(
            conn->get_fd(),
            ud->rpc_message->get_buffer(),
            ud
        );
    } else {
        LOG(FATAL) << "Received credit but no queued request";
    }
}

void State::ppm_client(bool dn_resp, Buffer* dn_resp_buffer) {
    if (dn_resp) {
        if (valid_credit(dn_resp_buffer->data.get())) {
            // we have received a credit
            DLOG(INFO) << "Received a credit";
            send_from_ppm_queue();
        }
    } else {
        if (stats.sidecar_resp_in) {
            // we have received a response from the remote server
            // send a DN
            if (ppm_queue.size() - ppm_state.sent_dns + ppm_state.received_dns > 0) {
                DLOG(INFO) << "Send DN after receiving a response";
                send_dn();
            }
        }

        // This part assumes no failures
        int new_dns = ppm_queue.size() - ppm_state.sent_dns + ppm_state.received_dns;
        if (new_dns > 0) {
            // we have at least one request to send
            for (int i = 0; i < new_dns; i++) {
                // send DN
                DLOG(INFO) << "Send DN for new requests";
                send_dn();
            }
        }
    }
    
}

void State::send_dn() {
    // send a demand notification
    char msg[] = {0x01, 0x00, 0x01};
    udp_send(msg, config.endpoint_host, config.endpoint_port);
    ppm_state.sent_dns++;
    DLOG(INFO) << "Sent demand notification";
}

PPMState::PPMState()
:   sent_credits(0),
    sent_dns(0),
    received_dns(0) {}

inline static void write_dn_response(int result, Buffer* resp) {
    resp->data.get()[0] = 0x01; // demand notification
    resp->data.get()[1] = 0x01; // response
    resp->data.get()[2] = result;
    resp->set_filled(3);
    DLOG(INFO) << "Wrote a DN response";
}

void State::queue_multiplexer(Buffer* req, Buffer* resp) {
    // read the request
    if (req->data.get()[0] == 0x01) {
        // we have demand notification

        // check if it's a request
        if (req->data.get()[1] != 0x00) {
            LOG(FATAL) << "QM only handles DN requests";
        }

        // produce the response
        uint8_t result = 0;
        if (config.ppm_limit > ppm_state.sent_credits - stats.app_resp_out) {
            result = 1;
            ppm_state.sent_credits++;
        }

        // write the response
        write_dn_response(result, resp);
    } else {
        LOG(FATAL) << "Unknown message type";
    }
}

void State::udp_send(std::span<char> msg, std::string& host, uint16_t port) {
    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(host.c_str());

    if (inet_pton(AF_INET, host.c_str(), &servaddr.sin_addr) <= 0) {
        LOG(FATAL) << "Invalid server IP address";
        return;
    }

    // write the message to a buffer
    Buffer* buffer = buffer_manager.get_buffer();
    std::memcpy(buffer->data.get(), msg.data(), msg.size());
    buffer->set_filled(msg.size());
    
    // send the request using the ring
    ring.prepare_req_sendmsg(
        sockfd,
        buffer,
        buffer_manager.get_user_data(),
        servaddr
    );

    // preprae rcvmsg for the response
    ring.prepare_rcvmsg(
        sockfd,
        buffer_manager.get_buffer(),
        buffer_manager.get_user_data(),
        UDPType::RESPONSE
    );
}