#include "state.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "connection_enums.h"
#include "glog/logging.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_queue.h"
#include <algorithm>
#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <memory>
#include <unordered_map>

State::State(Config config, RingWrapper& ring, BufferManager& buffer_manager,
    RPCMapper& mapper, RPCQueue& queue, std::unordered_map<ConnectionType, Listener>& listeners)
:   config(config),
    pools(),
    stats(),
    ppm_state(),
    buffer_manager(buffer_manager),
    ring(ring),
    rpc_mapper(mapper),
    rpc_queue(queue),
    listeners(listeners)
{
    pools.emplace(ConnectionType::EGRESS, ConnectionPool(ConnectionType::EGRESS));
    pools.emplace(ConnectionType::INGRESS, ConnectionPool(ConnectionType::INGRESS));

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }
}

void State::write_http(HTTPConnection* conn) {
    if (conn->want_write() == 0) {
        return;
    }
    DLOG(INFO) << "Starting to write batch of HTTP/2 data on fd: " << conn->get_fd();

    while (conn->want_write()) {
        auto send_buffer = buffer_manager.get_buffer();
        conn->http_write(send_buffer);

        if (send_buffer->get_filled() == 0) {
            buffer_manager.free_buffer(send_buffer);
            break;
        }

        auto write_ud = buffer_manager.get_user_data();
        prepare_write(write_ud, send_buffer, conn);

        // prepare write (to write the request)
        ring.prepare_write(
            conn->get_fd(),
            send_buffer,
            write_ud
        );
    }

    DLOG(INFO) << "Finished writing batch of HTTP/2 data written on fd: " << conn->get_fd();
}

void State::report_latency(RPCMessage& rpc, ConnectionType type) {
    // calculate the duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - rpc.req_rcv_time);

    LOG(INFO) << "E2E Latency: " << duration.count() << " us, " 
            << " type: " << type_to_str(type);
    
    LOG(INFO) << "Request forward delay: " << std::chrono::duration_cast<std::chrono::microseconds>(
        rpc.req_for_time - rpc.req_rcv_time).count() << " us";

    LOG(INFO) << "Response forward delay: " << std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - rpc.res_rcv_time).count() << " us";
}


bool State::route_request(uint32_t stream_id, ConnectionType type) {
    // get the RPC message
    auto& rpc = rpc_mapper.get_ds_rpc(type, stream_id);

    // get an upstream connection
    try {
        auto& conn = pools.at(type).get_any_connection();

        if (conn->get_status() == ConnectionStatus::TEARDOWN) {
            LOG(WARNING) << "Connection is in TEARDOWN state. Starting a new connection.";
            throw NoConnectionException();
        } else if (conn->get_status() == ConnectionStatus::DOWN) {
            LOG(WARNING) << "Connection is in DOWN state.";
            // put the RPC message back in the queue
            rpc_queue.enqueue(type, ConnectionDirection::DOWNSTREAM, stream_id);
            return false;
        }

        // submit the request
        rpc->us_stream_id = conn->submit_request(*rpc.get());

        // update the mapping
        rpc_mapper.route(type, rpc->ds_stream_id, rpc->us_stream_id);

        // record the time
        rpc->req_for_time = std::chrono::system_clock::now();

        // update ppm state
        if (type == ConnectionType::EGRESS) {
            ppm_state.unused_credits--;
        }

        // flush the request
        write_http(conn.get());
        
        DLOG(INFO) << "Submitted request on stream " << rpc->us_stream_id;
        return true;

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
            host, port, &rpc_mapper, &rpc_queue);
        DLOG(INFO) << "New connection established on fd: " << conn->get_fd();
        
        // prepare connect
        ring.prepare_connect(conn, buffer_manager.get_user_data());

        // put the RPC message back in the queue
        rpc_queue.enqueue(type, ConnectionDirection::DOWNSTREAM, stream_id);

        return false;
    }
}


void State::route(ConnectionType type, ConnectionDirection direction) {
    DLOG(INFO) << "Starting routing on type " << type_to_str(type) << " and direction " << direction_to_str(direction);

    // check if we have any RPC message in the queue
    while (!rpc_queue.empty(type, direction)) {
        auto stream_id = rpc_queue.dequeue(type, direction);
        DLOG(INFO) << "Routing message on stream " << stream_id << " of type " << type_to_str(type)
                   << " and direction " << direction_to_str(direction);

        if (direction == ConnectionDirection::DOWNSTREAM) {
            // we are dealing with a request

            if (type == ConnectionType::EGRESS) {
                // this should be handled by ppm client
                rpc_queue.enqueue(type, direction, stream_id);
                DLOG(INFO) << "PPM client should route EGRESS requests";
                return;
            }

            if (!route_request(stream_id, type)) {
                break;
            }
        }
        else if (direction == ConnectionDirection::UPSTREAM) {
            // we are dealing with a response

            if (listeners.at(type).no_connections()) {
                LOG(WARNING) << "No " << listeners.at(type).type_to_str() << " connections available";
                DLOG(INFO) << "Finished routing on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
                return;
            }

            // get the RPC message
            auto& rpc = rpc_mapper.get_us_rpc(type, stream_id);
            try {
                auto& conn = listeners.at(type).get_connections().at(rpc->ds_fd);
                //auto conn = listeners.at(type).get_connections().begin()->second.get();

                // submit the response
                conn->submit_response(*rpc.get());
                write_http(conn.get());
                DLOG(INFO) << "Submitted response on stream " << rpc->ds_stream_id;

                // report latency
                report_latency(*rpc.get(), type);

                // update stats
                stats.sidecar_resp_in[type]++;
                if (type == ConnectionType::EGRESS) {
                    stats.new_response_in = true;
                }

                // remove the RPC message from memory
                rpc_mapper.remove_rpc(type, rpc->ds_stream_id);
                
            } catch (const std::out_of_range& e) {
                LOG(FATAL) << "No connection found for fd: " << rpc->ds_fd;
            }
            
        }
    }

    DLOG(INFO) << "Finished routing on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
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

bool State::valid_credit(const char* data) {
    if (data[0] != 0x01) {
        LOG(FATAL) << "Invalid message type";
    }

    if (data[1] != 0x01) {
        LOG(FATAL) << "Expected a response";
    }

    ppm_state.received_dns++;
    if (data[2] == 0x01) {
        ppm_state.unused_credits++;
        ppm_state.received_credits++;
        return true;
    } else {
        return false;
    }
    //return data[2] == 0x01;
}


/* void State::send_from_ppm_queue() {
    if (ppm_queue.size() > 0) {
        DLOG(INFO) << "Send a request from PPM queue";

        // send one queued request (from the end of queue)
        auto& msg = ppm_queue.front();

        // send the request using io-uring
        // Note that we don't need to do any routing here
        auto& conn = pools.at(ConnectionType::EGRESS).get_any_connection();
        msg->get_buffer()->prepare_write(conn.get());
        auto ud = buffer_manager.get_user_data();
        ud->rpc_message = std::move(msg);
        // remove the first element from the queue
        ppm_queue.erase(ppm_queue.begin());

        ring.prepare_write(
            conn->get_fd(),
            ud->rpc_message->get_buffer(),
            ud
        );
    } else {
        LOG(FATAL) << "Received credit but no queued request";
    }
} */

void State::ppm_client(bool dn_resp, Buffer* dn_resp_buffer) {
    if (dn_resp) {
        if (valid_credit(dn_resp_buffer->data.get())) {
            // we have received a credit
            DLOG(INFO) << "PPMClient: Received a credit";

            // send a request from the queue
            route_request(
                rpc_queue.dequeue(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM),
                ConnectionType::EGRESS
            );
        } else {
            DLOG(INFO) << "PPMClient: Received a non-credit response";
        }
    } else {
        int size = rpc_queue.size(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);
        if (size == 0) {
            return;
        }

        if (ppm_state.unused_credits > 0) {
            int to_send = std::min(ppm_state.unused_credits, size);
            for (int i = 0; i < to_send; i++) {
                if (!route_request(
                    rpc_queue.dequeue(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM),
                    ConnectionType::EGRESS)) {
                    return;
                }
            }
        }
        

        if (stats.new_response_in) {
            // we have received a response from the remote server
            // send a DN
            if (rpc_queue.size(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM)
                - ppm_state.sent_dns + ppm_state.received_dns > 0) {
                DLOG(INFO) << "PPMClient: Send DN after receiving a response";
                stats.new_response_in = false;
                send_dn();
            }
        }

        // This part assumes no failures
        int new_dns = rpc_queue.size(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM)
                        - ppm_state.sent_dns + ppm_state.received_credits;
        if (new_dns > 0) {
            // we have at least one request to send
            for (int i = 0; i < new_dns; i++) {
                // send DN
                DLOG(INFO) << "PPMClient: Send DN for new requests";
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
    received_credits(0),
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
        if (config.ppm_limit > ppm_state.sent_credits - 
            stats.sidecar_resp_in[ConnectionType::INGRESS]) {
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