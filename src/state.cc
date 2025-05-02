#include "state.h"
#include "buffer.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "connection_enums.h"
#include "glog/logging.h"
#include "ppm_queue.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unordered_map>
#include <utility>

UpstreamRouteMapper::UpstreamRouteMapper()
: map() {}

void UpstreamRouteMapper::add_route(std::string service) {
    if (map.find(service) == map.end()) {
        map.emplace(service, ConnectionPool(ConnectionType::EGRESS));
    }
}

ConnectionPool& UpstreamRouteMapper::get_pool(const std::string& service) {
    if (map.find(service) == map.end()) {
        if (map.find("*") != map.end()) {
            return map.at("*");
        }
        LOG(FATAL) << "Service not found in routing table: " << service;
    } else {
        return map.at(service);
    }
}

State::State(Config config, RingWrapper& ring, BufferManager& buffer_manager,
    RPCMapper& mapper, RPCQueue& queue, std::unordered_map<ConnectionType, Listener>& listeners)
:   config(config),
    ingress_pool(ConnectionType::INGRESS),
    upstream_route_mapper(),
    stats(),
    ppm_state(),
    buffer_manager(buffer_manager),
    ring(ring),
    rpc_mapper(mapper),
    rpc_queue(queue),
    listeners(listeners),
    ppm_queue()
{
    for (const auto& route : config.routing) {
        upstream_route_mapper.add_route(route.service);
        auto& conn = upstream_route_mapper.get_pool(route.service).add_connection(
            route.upstream.host,
            route.upstream.port,
            &rpc_mapper,
            &rpc_queue
        );

        // prepare connect
        ring.prepare_connect(conn, buffer_manager.get_user_data());

        ppm_state.denied_reqs.emplace(route.service, 0);
    }

    if (!config.disable_ingress) {
        auto& conn = ingress_pool.add_connection(
            config.ingress_upstream_host,
            config.ingress_upstream_port,
            &rpc_mapper,
            &rpc_queue
        );
    
        // prepare connect
        ring.prepare_connect(conn, buffer_manager.get_user_data());
    }
    
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }
}

void State::write_http(HTTPConnection* conn) {
    if (conn->want_write() == 0) {
        return;
    }
    VLOG(1) << "Starting to write batch of HTTP/2 data on fd: " << conn->get_fd();

    Buffer* send_buffer = nullptr;
    bool write_flag = false;
    while (conn->want_write()) {
        if (send_buffer == nullptr) {
            send_buffer = buffer_manager.get_buffer();
        }

        conn->http_write(send_buffer);

        if (send_buffer->get_filled() == 0) {
            buffer_manager.free_buffer(send_buffer);
            break;
        }
        write_flag = true;
    }

    if (write_flag) {
        auto write_ud = buffer_manager.get_user_data();
        prepare_write(write_ud, send_buffer, conn);

        // prepare write (to write the request)
        ring.prepare_write(
            conn->get_fd(),
            send_buffer,
            write_ud
        );
    }

    VLOG(1) << "Finished writing batch of HTTP/2 data written on fd: " << conn->get_fd();
}

void State::report_latency(RPCMessage& rpc, ConnectionType type) {
    // calculate the duration
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - rpc.req_rcv_time);
    
    LOG(INFO) << "M# " << config.name << " E2E-" << type_to_str(type) << " " << duration.count();
    
    LOG(INFO) << "M# " << config.name << " REQ-FOR " << std::chrono::duration_cast<std::chrono::microseconds>(
        rpc.req_for_time - rpc.req_rcv_time).count();

    LOG(INFO) << "M# " << config.name << " RES-FOR " << std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - rpc.res_rcv_time).count();
}

HTTPConnection* State::route_request(ConnectionType type, uint32_t ds_stream_id, int ds_fd) {
    // get the RPC message
    auto& rpc = rpc_mapper.get_ds_rpc(type, ds_stream_id, ds_fd);

    try {
        HTTPConnection* conn = nullptr;
        // TODO: implement load balancing within ach pool
        if (type == ConnectionType::INGRESS) {
            conn = ingress_pool.get_any_connection().get();
        } else {
            conn = upstream_route_mapper.get_pool(rpc->get_service()).get_any_connection().get();
        }

        if (conn->get_status() == ConnectionStatus::TEARDOWN) {
            LOG(WARNING) << "Connection is in TEARDOWN state. Starting a new connection.";
            throw NoConnectionException();
        } else if (conn->get_status() == ConnectionStatus::DOWN) {
            LOG(WARNING) << "Connection is in DOWN state.";
            // put the RPC message back in the queue
            rpc_queue.enqueue(type, ConnectionDirection::DOWNSTREAM, rpc->ds_fd, rpc->ds_stream_id);
            return nullptr;
        }

        // update RPC message metadata
        rpc->us_fd = conn->get_fd();

        VLOG(1) << "Routing message (" << ds_fd << "," << ds_stream_id << ") to fd: " << conn->get_fd();

        return conn;
    }
    catch (const std::exception& e) {
        LOG(FATAL) << "Error in routing, " << e.what();
    }
}


bool State::forward_request(HTTPConnection* conn, std::shared_ptr<RPCMessage>& rpc) {
    // get an upstream connection
    try {

        // submit the request
        rpc->us_stream_id = conn->submit_request(*rpc.get());

        // update the mapping
        rpc_mapper.route(conn->type, rpc->ds_stream_id, rpc->ds_fd, rpc->us_stream_id, conn->get_fd());

        // record the time
        rpc->req_for_time = std::chrono::system_clock::now();
        LOG(INFO) << "M# " << config.name << " REQ 1";

        /* // update ppm state
        if (type == ConnectionType::EGRESS) {
            ppm_state.unused_credits--;
        } */

        // flush the request
        write_http(conn);
        
        VLOG(1) << "Submitted request on stream " << rpc->us_stream_id;
        return true;

    } catch (NoConnectionException& e) {
        LOG(FATAL) << "No connection available. Starting a new connection.";

        /* std::string host;
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
        VLOG(1) << "New connection established on fd: " << conn->get_fd();
        
        // prepare connect
        ring.prepare_connect(conn, buffer_manager.get_user_data());

        // put the RPC message back in the queue
        rpc_queue.enqueue(type, ConnectionDirection::DOWNSTREAM, stream_id);

        return false; */
    }
}


void State::forward(ConnectionType type, ConnectionDirection direction) {
    if (type == ConnectionType::EGRESS && direction == ConnectionDirection::DOWNSTREAM) {
        // we don't route EGRESS DOWNSTREAM requests
        return;
    }
    if (rpc_queue.empty(type, direction)) {
        return;
    }
    VLOG(1) << "Starting forwarding on type " << type_to_str(type) << " and direction " << direction_to_str(direction);

    // check if we have any RPC message in the queue
    while (!rpc_queue.empty(type, direction)) {
        auto [src_fd, src_stream_id] = rpc_queue.dequeue(type, direction);
        VLOG(1) << "Routing message (" << src_fd << "," << src_stream_id << ") of type " << type_to_str(type)
                   << " and direction " << direction_to_str(direction);

        if (direction == ConnectionDirection::DOWNSTREAM) {
            // we are dealing with a request

            /* if (type == ConnectionType::EGRESS) {
                // this should be handled by ppm client
                rpc_queue.enqueue(type, direction, fd, stream_id);
                VLOG(1) << "PPM client should route EGRESS requests";
                return;
            } */

            HTTPConnection* conn = route_request(type, src_stream_id, src_fd);
            auto& rpc = rpc_mapper.get_ds_rpc(type, src_stream_id, src_fd);

            if (!forward_request(conn, rpc)) {
                break;
            }
        }
        else if (direction == ConnectionDirection::UPSTREAM) {
            // we are dealing with a response

            if (listeners.at(type).no_connections()) {
                LOG(WARNING) << "No " << listeners.at(type).type_to_str() << " connections available";
                VLOG(1) << "Finished routing on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
                return;
            }

            // get the RPC message
            auto& rpc = rpc_mapper.get_us_rpc(type, src_stream_id, src_fd);
            try {
                auto& conn = listeners.at(type).get_connections().at(rpc->ds_fd);
                //auto conn = listeners.at(type).get_connections().begin()->second.get();

                // submit the response
                conn->submit_response(*rpc.get());
                write_http(conn.get());
                VLOG(1) << "Submitted response on stream " << rpc->ds_stream_id;

                // report latency
                report_latency(*rpc.get(), type);

                // update stats
                stats.sidecar_resp_in[type]++;
                if (type == ConnectionType::EGRESS) {
                    stats.new_response_in = true;
                }

                // remove the RPC message from memory
                rpc_mapper.remove_rpc(type, rpc);
                
            } catch (const std::out_of_range& e) {
                LOG(FATAL) << "No connection found for fd: " << rpc->ds_fd;
            }
            
        }
    }

    VLOG(1) << "Finished forwarding on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
}

/* std::unique_ptr<HTTPConnection>& State::get_connection(int fd, ConnectionType type) {
    return  pools.at(type).get_connection(fd);
} */

/* void State::remove_one_connection(ConnectionType type) {
    pools.at(type).remove_connection(pools.at(type).get_any_connection()->get_fd());
} */

/* HTTPConnection& State::get_one_connection(ConnectionType type) {
    return *pools.at(type).get_any_connection().get();
} */

void State::remove_connection(HTTPConnection& conn) {
    LOG(FATAL) << "Removing connection for upstream is not implemented";
    //pools.at(conn.type).remove_connection(conn.get_fd());
}

std::pair<const std::string&, bool> State::valid_credit(const char* data) {
    if (data[1] != 0x01) {
        LOG(FATAL) << "Invalid message type";
    }

    if (data[2] != 0x01) {
        LOG(FATAL) << "Expected a response";
    }
    std::string_view key(data+4);
    if (data[3] == 0x01) {
        
        return {ppm_queue.check(key), true};
    } else {
        auto it = ppm_state.denied_reqs.find(key);
        if (it != ppm_state.denied_reqs.end()) {
            it->second++;
            return {it->first, false};
        } else {
            throw std::runtime_error("key not found");
        }
    }
}


/* void State::send_from_ppm_queue() {
    if (ppm_queue.size() > 0) {
        VLOG(1) << "Send a request from PPM queue";

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
        // we have received a demand notification response
        auto [service, ok] = valid_credit(dn_resp_buffer->data.get());
        if (!ok) {
            // we have received a demand notification response but no credit
            VLOG(1) << "PPMClient: Credit denied for service " << service;
            return;
        }
        // we have received a credit
        VLOG(1) << "PPMClient: Received a credit for service " << service;

        auto rpc = ppm_queue.dequeue(service);
        forward_request(
            upstream_route_mapper.get_pool(service).get_connection(rpc->us_fd).get(),
            rpc
        );
    } else {
        // we need to send a demand notification
        int size = rpc_queue.size(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);
        if (size == 0) {
            return;
        }

        if (stats.new_response_in) {
            for (auto& key : ppm_state.denied_reqs) {
                if (key.second > 0) {
                    VLOG(1) << "PPMClient: Sending demand notification for service " << key.first;
                    send_dn(
                        upstream_route_mapper.get_pool(key.first).get_any_connection().get(),
                        key.first
                        );
                    key.second--;
                    break;
                }
            }
            stats.new_response_in = false;
        }

        for (int i = 0; i < size; i++) {
            // send a demand notification
            auto [ds_fd, ds_stream_id] = rpc_queue.dequeue(
                ConnectionType::EGRESS,
                ConnectionDirection::DOWNSTREAM
            );
            auto conn = route_request(ConnectionType::EGRESS, ds_stream_id, ds_fd);
            auto& rpc = rpc_mapper.get_ds_rpc(
                ConnectionType::EGRESS,
                ds_stream_id,
                ds_fd
            );
            ppm_queue.enqueue(rpc);
            send_dn(conn, rpc->get_service());
            VLOG(1) << "PPMClient: Send DN for queued requests";
        }
    }  
}

void State::send_dn(HTTPConnection* conn, const std::string& service) {
    // send a demand notification
    char len = 4 + service.length();
    char msg[len];
    msg[0] = len;
    msg[1] = 0x01; // demand notification (0x01)
    msg[2] = 0x00; // request (0x00), response (0x01)
    msg[3] = 0x01; // number of credits
    std::memcpy(msg + 4, service.c_str(), service.length());
    udp_send(std::span<char>(msg, len), conn->get_host(), conn->get_port());
    //ppm_state.sent_dns++;
    VLOG(1) << "Sent demand notification";
}

PPMState::PPMState()
:   sent_credits(0),
    denied_reqs(std::unordered_map<std::string, uint8_t, TransparentHash, TransparentEqual>()) {}

inline static void write_dn_response(int result, Buffer* req, Buffer* resp) {
    // copy request to response
    std::memcpy(resp->data.get(), req->data.get(), req->get_filled());
    //resp->data.get()[1] = 0x01; // demand notification
    resp->data.get()[2] = 0x01; // response
    resp->data.get()[3] = result;
    resp->set_filled(req->get_filled());
    VLOG(1) << "Wrote a DN response";
}

void State::queue_multiplexer(Buffer* req, Buffer* resp) {
    // read the request
    if (req->data.get()[1] == 0x01) {
        // we have demand notification

        // check if it's a request
        if (req->data.get()[2] != 0x00) {
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
        write_dn_response(result, req, resp);
    } else {
        LOG(FATAL) << "Unknown message type";
    }
}

void State::udp_send(std::span<char> msg, std::string& host, uint16_t port) {
    struct addrinfo hints, *res;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; // IPv4
    hints.ai_socktype = SOCK_DGRAM;

    int ret = getaddrinfo(host.c_str(), nullptr, &hints, &res);
    if (ret != 0) {
        LOG(FATAL) << "DNS resolution failed for host " << host << ": " << gai_strerror(ret);
        return;
    }

    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr = ((struct sockaddr_in*)res->ai_addr)->sin_addr;
    freeaddrinfo(res);

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