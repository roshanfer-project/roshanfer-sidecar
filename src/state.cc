#include "state.h"
#include "buffer.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "connection_enums.h"
#include "glog/logging.h"
#include "hdr/hdr_histogram.h"
#include "ppm_queue.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include <algorithm>
#include <arpa/inet.h>
#include <cstddef>
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
#include <vector>

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

State::State(Config parsed_config, RingWrapper& ring_ref, BufferManager& buffer_manager_ref,
    RPCMapper& mapper_ref, RPCQueue& queue_ref, std::unordered_map<ConnectionType,
    Listener>& listeners_ref, Ingress& ingress_ref)
:   config(parsed_config),
    ingress_pool(ConnectionType::INGRESS),
    upstream_route_mapper(),
    ring(ring_ref),
    buffer_manager(buffer_manager_ref),
    rpc_mapper(mapper_ref),
    rpc_queue(queue_ref),
    listeners(listeners_ref),
    ppm_queue(),
    ingress(ingress_ref),
    stats(config.routing),
    ppm_state()
{

    if (config.buffer_size > HTTP1Connection_BUF_SIZE) {
        LOG(FATAL) << "Buffer size cannot be larger than " << HTTP1Connection_BUF_SIZE;
    }

    for (const auto& route : config.routing) {
        upstream_route_mapper.add_route(route.service);
        auto& conn = upstream_route_mapper.get_pool(route.service).add_connection(
            route.upstream.host,
            route.upstream.port,
            &rpc_mapper,
            &rpc_queue,
            false, // HTTP/2 connection
            hist
        );

        // prepare connect
        ring.prepare_connect(conn, buffer_manager.get_user_data());

        ppm_state.denied_reqs.emplace(route.service, 0);
    }

    /* 
    Since HTTP/1.1 does not support multiplexing, we need to create at least
    ppm_limit connections for ingress requests. In the case of HTTP/2, we can
    easilly go up to 100 concurrent streams, but ppm_limit will limit the number 
    concurrent streams.
    */
    int n_conn = config.is_ingress ? config.ppm_limit : 1;
    VLOG(2) << "Creating " << n_conn << " connections for ingress requests";
    for (int i = 0; i< n_conn; i++) {
        auto& conn = ingress_pool.add_connection(
            config.ingress_upstream_host,
            config.ingress_upstream_port,
            &rpc_mapper,
            &rpc_queue,
            config.is_ingress,
            hist
        );

        // prepare connect
        ring.prepare_connect(conn, buffer_manager.get_user_data());
    }
    
    // socket for UDP (Used for PPM)
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    hdr_init(1, 50000, 3, &hist);
    next_hist_update = std::chrono::steady_clock::now() + std::chrono::seconds(1);
}

void State::write_http(HTTPConnection* conn) {
    if (conn->want_write() == 0) {
        return;
    }
    VLOG(1) << "Starting to write batch of HTTP data on fd: " << conn->get_fd();

    while (conn->want_write()) {
        Buffer* send_buffer = buffer_manager.get_buffer();

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

    VLOG(1) << "Finished writing batch of HTTP data written on fd: " << conn->get_fd();
}


HTTPConnection* State::route_request(ConnectionType type, int32_t ds_stream_id, int ds_fd) {
    // get the RPC message
    auto rpc = rpc_mapper.get_ds_rpc(type, ds_stream_id, ds_fd);

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
            throw NoConnectionException("Connection is in TEARDOWN state");
        } else if (conn->get_status() == ConnectionStatus::DOWN) {
            LOG(WARNING) << "Connection is in DOWN state.";
            // put the RPC message back in the queue
            rpc_queue.enqueue(type, ConnectionDirection::DOWNSTREAM, rpc->get_ds_fd(), rpc->get_ds_stream_id());
            return nullptr;
        }

        // update RPC message metadata
        rpc->set_us_fd(conn->get_fd());

        VLOG(1) << "Routing message (" << ds_fd << "," << ds_stream_id << ") to fd: " << conn->get_fd();

        return conn;
    }
    catch (NoConnectionException& e) {
        LOG(FATAL) << "No connection available for routing request: " << e.what()
                   << " type: " << type_to_str(type)
                   << " ingress_size: " << ingress.size();
    }
    catch (const std::exception& e) {
        LOG(FATAL) << "Error in routing, " << e.what();
    }
}


bool State::forward_request(HTTPConnection* conn, RPCMessage* rpc) {
    // get an upstream connection
    try {

        // submit the request
        rpc->set_us_stream_id(conn->submit_request(*rpc));

        // update the mapping
        rpc_mapper.route(conn->type, rpc->get_ds_stream_id(), rpc->get_ds_fd(), rpc->get_us_stream_id(), conn->get_fd());

        // flush the request
        write_http(conn);
        
        VLOG(1) << "Submitted request on stream " << rpc->get_us_stream_id();
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
            RPCMessage* rpc = rpc_mapper.get_ds_rpc(type, src_stream_id, src_fd);

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
            RPCMessage* rpc = rpc_mapper.get_us_rpc(type, src_stream_id, src_fd);
            int32_t ds_stream_id = rpc->get_ds_stream_id();
            int ds_fd = rpc->get_ds_fd();
            try {
                std::unique_ptr<HTTPConnection>& conn = listeners.at(type).get_connections().at(rpc->get_ds_fd());

                // update stats
                if (!rpc->is_drop()) {
                    stats.sidecar_resp_in.at(type)++;
                }
                if (type == ConnectionType::EGRESS) {
                    stats.new_response_in.at(rpc->get_service()) = true;
                }

                // submit the response
                if (rpc->is_error()) {
                    conn->submit_error_response(*rpc);
                    if (!rpc->is_http()) {
                        rpc_mapper.remove_rpc(type, rpc);
                    }
                } else {
                    conn->submit_response(*rpc);
                }
                write_http(conn.get());
                VLOG(1) << "Submitted response on stream " << ds_stream_id;


                
                
            } catch (const std::out_of_range& e) {
                LOG(FATAL) << "No connection found for fd: " << ds_fd;
            } catch (const std::exception& e) {
                LOG(FATAL) << "Error in routing response: " << e.what()
                           << " type: " << type_to_str(type)
                           << " direction: " << direction_to_str(direction)
                           << " stream_id: " << src_stream_id
                           << " fd: " << src_fd;
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

void State::remove_connection(HTTPConnection& /*conn*/) {
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
    if (data[0] < 4) {
        LOG(FATAL) << "Invalid message length: " << (int)data[0];
    }
    std::string_view key(data+4, (size_t)data[0] - 4);
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
        auto [service, ok] = valid_credit(dn_resp_buffer->data.data());
        if (!ok) {
            // we have received a demand notification response but no credit
            VLOG(1) << "PPMClient: Credit denied for service " << service;
            return;
        }
        // we have received a credit
        VLOG(1) << "PPMClient: Received a credit for service " << service;

        auto rpc = ppm_queue.dequeue(service);
        forward_request(
            upstream_route_mapper.get_pool(service).get_connection(rpc->get_us_fd()).get(),
            rpc
        );
    } else {
        // we need to send a demand notification
        for (auto & key : stats.new_response_in) {
            if (key.second && ppm_state.denied_reqs[key.first] > 0) {
                VLOG(1) << "PPMClient: Sending demand notification for service " << key.first;
                send_dn(
                    upstream_route_mapper.get_pool(key.first).get_any_connection().get(),
                    key.first
                );
                key.second = false;
                ppm_state.denied_reqs[key.first]--;
            }
        }

        size_t size = rpc_queue.size(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);
        for (size_t i = 0; i < size; i++) {
            // send a demand notification
            auto [ds_fd, ds_stream_id] = rpc_queue.dequeue(
                ConnectionType::EGRESS,
                ConnectionDirection::DOWNSTREAM
            );
            auto conn = route_request(ConnectionType::EGRESS, ds_stream_id, ds_fd);
            auto rpc = rpc_mapper.get_ds_rpc(
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

    size_t len = 4 + service.length();
    std::vector<char> msg(len);
    msg.at(0) = (char)len;
    msg.at(1) = 0x01; // demand notification (0x01)
    msg.at(2) = 0x00; // request (0x00), response (0x01)
    msg.at(3) = 0x01; // number of credits

    std::copy_n(service.begin(), service.length(), msg.begin() + 4);
    udp_send(msg, reinterpret_cast<struct sockaddr_in*>(conn->get_addr()));
    VLOG(1) << "Sent demand notification";
}

void State::ingress_admit() {
    // update ingress's p95 estimate
    if (std::chrono::steady_clock::now() >= next_hist_update) {
        ingress.update_p95(hdr_value_at_percentile(hist, 95.0));
        hdr_reset(hist);
        next_hist_update = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    }

    bool admit = false;
    auto tmp_size = ingress.size();
    for (size_t i = 0; i < tmp_size; i++) {
        if (config.ppm_limit > ppm_state.sent_credits - 
            stats.sidecar_resp_in[ConnectionType::INGRESS]) {
            
            VLOG(1) << "Admitting an ingress request";
            admit = true;
            ppm_state.sent_credits++;
            auto rpc = ingress.dequeue();
            rpc_queue.enqueue(
                ConnectionType::INGRESS,
                ConnectionDirection::DOWNSTREAM,
                rpc->get_ds_fd(),
                rpc->get_ds_stream_id()
            );
        } else {
            break;
        }
    }

    if (admit) {
        forward(ConnectionType::INGRESS, ConnectionDirection::DOWNSTREAM);
    }

    // check for any potential dropping
    if (ingress.check_drop(rpc_queue, rpc_mapper)) {
        forward(ConnectionType::INGRESS, ConnectionDirection::UPSTREAM);
    }
    
}

inline static void write_dn_response(int result, Buffer* req, Buffer* resp) {
    // copy request to response
    std::copy_n(req->data.begin(), req->get_filled(), resp->data.begin());
    resp->data.at(2) = 0x01; // response
    resp->data.at(3) = (char)result;
    resp->set_filled(req->get_filled());
    VLOG(1) << "Wrote a DN response";
}

void State::queue_multiplexer(Buffer* req, Buffer* resp) {
    // read the request
    if (req->data.at(1) == 0x01) {
        // we have demand notification

        // check if it's a request
        if (req->data.at(2) != 0x00) {
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

void State::udp_send(std::vector<char> msg, struct sockaddr_in* addr) {

    // write the message to a buffer
    Buffer* buffer = buffer_manager.get_buffer();
    std::copy_n(msg.begin(), msg.size(), buffer->data.begin());
    buffer->set_filled(msg.size());
    
    // send the request using the ring
    ring.prepare_req_sendmsg(
        sockfd,
        buffer,
        buffer_manager.get_user_data(),
        *addr
    );

    // preprae rcvmsg for the response
    ring.prepare_rcvmsg(
        sockfd,
        buffer_manager.get_buffer(),
        buffer_manager.get_user_data(),
        UDPType::RESPONSE
    );
}


PPMState::PPMState()
:   sent_credits(0),
    denied_reqs(std::unordered_map<std::string, uint8_t, TransparentHash, TransparentEqual>()) {}