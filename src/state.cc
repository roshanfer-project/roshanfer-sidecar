#include "state.h"
#include "buffer.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "connection_enums.h"
#include "fast_map.hpp"
#include "glog/logging.h"
#include "hdr/hdr_histogram.h"
#include "ppm_queue.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <algorithm>
#include <arpa/inet.h>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <tuple>
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
        // log keys
        for (const auto& [key, _] : map) {
            LOG(INFO) << "Available route: " << key;
        }
        LOG(FATAL) << "Service not found in routing table: " << service;
    } else {
        return map.at(service);
    }
}

LocalState create_local_state(const Config& local_config) {
    std::vector<std::string> hosted_services;
    for (const auto& mapping : local_config.mapping) {
        hosted_services.push_back(mapping.first);
    }

    std::vector<std::string> downstream_services;
    for (const auto& [route, _] : local_config.routing) {
        downstream_services.push_back(route);
    }

    return LocalState(hosted_services, downstream_services);
}

Utilization create_utilization(const Config& local_config) {
    std::vector<std::string> hosted_services;
    for (const auto& mapping : local_config.mapping) {
        hosted_services.push_back(mapping.first);
    }
    return Utilization(1000, hosted_services);
}

State::State(Config parsed_config, RingWrapper& ring_ref, BufferManager& buffer_manager_ref,
    RPCMapper& mapper_ref, RPCQueue& queue_ref, std::unordered_map<ConnectionType,
    std::shared_ptr<Listener>>& listeners_ref, Ingress& ingress_ref, SharedState& shared_state_ref, std::string& ingress_service_ref)
:   config(parsed_config),
    ingress_pool(ConnectionType::INGRESS),
    upstream_route_mapper(),
    ring(ring_ref),
    buffer_manager(buffer_manager_ref),
    rpc_mapper(mapper_ref),
    rpc_queue(queue_ref),
    listeners(listeners_ref),
    ppm_queue(config.routing),
    ingress(ingress_ref),
    hist(),
    shared_state(shared_state_ref),
    local_state(create_local_state(parsed_config)),
    utilization(create_utilization(parsed_config)),
    ingress_service(ingress_service_ref)
{

    if (config.buffer_size > HTTP1Connection_BUF_SIZE) {
        LOG(FATAL) << "Buffer size cannot be larger than " << HTTP1Connection_BUF_SIZE;
    }

    for (const auto& [route, info] : config.routing) {
        upstream_route_mapper.add_route(route);
        auto& pool = upstream_route_mapper.get_pool(route);
        int n_conn = config.is_ingress ? config.ingress_pool_connections.value() : 1;
        auto http_type = config.is_ingress ? HTTP::HTTP1 : HTTP::HTTP2;
        for (int i = 0; i < n_conn; i++) {
            auto conn = pool.add_connection(
                info.upstream.host,
                info.upstream.port,
                &rpc_mapper,
                &rpc_queue,
                http_type,
                hist
            );

            // prepare connect
            ring.prepare_connect(std::move(conn), buffer_manager.get_user_data());
        }
    }

    for (const auto& [service, info] : config.mapping) {
        local_state.local_concurrency_limit.set(service, (uint32_t)config.ppm_limit);

        // for ingress, we should have the same name for hosted services and downstream services
        if (config.is_ingress) {
            if (info.downstreams.size() != 1 || info.downstreams.at(0) != service) {
                LOG(FATAL) << "For hosted service: " << service
                           << ", downstreams: " << info.downstreams.at(0)
                           << " but should be the same";
            }
        }
    }

    /* 
    Since HTTP/1.1 does not support multiplexing, we need to create at least
    ppm_limit connections for ingress requests. In the case of HTTP/2, we can
    easilly go up to 100 concurrent streams, but ppm_limit will limit the number 
    concurrent streams.
    */
    int n_conn;
    if (config.is_frontend) {
        n_conn = config.frontend_pool_connections.value();
    } else if (config.is_ingress) {
        n_conn = 0;
    } else {
        n_conn = 1; // HTTP/2 connections can multiplex multiple streams
    }
    
    VLOG(2) << "Creating " << n_conn << " connections for ingress requests";
    for (int i = 0; i< n_conn; i++) {
        auto conn = ingress_pool.add_connection(
            config.ingress_upstream_host,
            config.ingress_upstream_port,
            &rpc_mapper,
            &rpc_queue,
            (config.is_ingress || config.is_frontend) ? HTTP::HTTP1 : HTTP::HTTP2,
            hist
        );

        // prepare connect
        ring.prepare_connect(std::move(conn), buffer_manager.get_user_data());
    }
    
    // socket for UDP (Used for PPM)
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        LOG(FATAL) << "Failed to create socket";
    }

    if (int ret = hdr_init(1, 1000000, 3, &hist); ret < 0) {
        LOG(FATAL) << "Failed to initialize histogram: " << strerror(ret);
    }
    next_hist_update = std::chrono::steady_clock::now() + std::chrono::seconds(1);
}

FailedDNInfoUnit::FailedDNInfoUnit(struct sockaddr_in addr, int num_rejected_requests)
: addr(std::make_unique<struct sockaddr_in>(addr)), num_rejected_requests(num_rejected_requests) {}

void State::write_http(std::shared_ptr<HTTPConnection> conn) {
    if (conn->want_write() == 0) {
        return;
    }
    VLOG(3) << "Starting to write batch of HTTP data on fd: " << conn->get_fd();

    if (conn->http() == HTTP::HTTP2) {
        std::unique_ptr<Buffer> send_buffer;
        bool write_flag = false;
        while (conn->want_write()) {
            if (!send_buffer) {
                send_buffer = buffer_manager.get_buffer();
            }

            try {
                conn->http_write(send_buffer);
            } catch (const BufferFullException& e) {
                // prepare existing buffer for write
                ring.prepare_write(
                    conn,
                    std::move(send_buffer),
                    buffer_manager.get_user_data()
                );
                // get a new buffer
                send_buffer = buffer_manager.get_buffer();
                // write into new one
                if ((size_t)e.written > send_buffer->get_size() - send_buffer->get_filled()) {
                    LOG(FATAL) << "Buffer too small for a single write, written: " << e.written
                    << ", buffer size: " << send_buffer->get_size()
                    << ", filled: " << send_buffer->get_filled();
                }
                std::copy_n(e.outbuf_ptr,
                    (size_t)e.written,
                send_buffer->data.begin() + (long)send_buffer->get_filled());
                send_buffer->set_filled(send_buffer->get_filled() + (size_t)e.written);
            }

            if (send_buffer->get_filled() == 0) {
                buffer_manager.free_buffer(std::move(send_buffer));
                break;
            }
            write_flag = true;
        }

        if (write_flag) {
            // prepare write (to write the request)
            ring.prepare_write(
                conn,
                std::move(send_buffer),
                buffer_manager.get_user_data()
            );
        }
    } else if (conn->http() == HTTP::HTTP1) {
        while (conn->want_write()) {
            auto send_buffer = buffer_manager.get_buffer();
            try {
                conn->http_write(send_buffer);
            } catch (const BufferFullException& e) {
                // NOTE: This should not happen (fix it like HTTP/2 if you have to)
                LOG(FATAL) << "Buffer full, written: " << e.written
                << ", buffer size: " << send_buffer->get_size()
                << ", filled: " << send_buffer->get_filled();
                break;
            }
            //conn->http_write(send_buffer);
            if (send_buffer->get_filled() == 0) {
                buffer_manager.free_buffer(std::move(send_buffer));
                break;
            }

            ring.prepare_write(
                conn,
                std::move(send_buffer),
                buffer_manager.get_user_data()
            );
        }
    } else {
        LOG(FATAL) << "Unknown HTTP type";
    }

    

    VLOG(3) << "Finished writing batch of HTTP data written on fd: " << conn->get_fd();
}


std::shared_ptr<HTTPConnection> State::route_request(ConnectionType type, int32_t ds_stream_id, int ds_fd) {
    // get the RPC message
    auto rpc = rpc_mapper.get_ds_rpc(type, ds_stream_id, ds_fd);

    try {
        std::shared_ptr<HTTPConnection> conn;
        // TODO: implement load balancing within each connection pool
        if (type == ConnectionType::INGRESS) {
            conn = ingress_pool.get_any_connection();
        } else {
            conn = upstream_route_mapper.get_pool(rpc->get_service()).get_any_connection();
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

        VLOG(3) << "Routing request"
                << " of type: " << type_to_str(type)
                << " service: " << rpc->get_service()
                << " message (" << ds_fd << "," << ds_stream_id << ") to fd: " << conn->get_fd();

        return conn;
    }
    catch (NoConnectionException& e) {
        dump_entire_state();
        LOG(FATAL) << "No connection available for routing request: " << e.what()
                   << " type: " << type_to_str(type)
                   << " service: " << rpc->get_service();
    }
    catch (const std::exception& e) {
        LOG(FATAL) << "Error in routing, " << e.what();
    }
}


bool State::forward_request(std::shared_ptr<HTTPConnection> conn, std::shared_ptr<RPCMessage> rpc) {
    // get an upstream connection
    try {

        // submit the request
        rpc->set_us_stream_id(conn->submit_request(rpc));

        // update the mapping
        rpc_mapper.route(conn->type, rpc->get_ds_stream_id(), rpc->get_ds_fd(), rpc->get_us_stream_id(), conn->get_fd());

        // flush the request
        write_http(conn);
        
        VLOG(3) << "Submitted request on stream " << rpc->get_us_stream_id();
        return true;
    } catch (NoConnectionException& e) {
        LOG(FATAL) << "No connection available. Starting a new connection.";
    } catch (std::exception& e) {
        LOG(FATAL) << "Error in forwarding request: " << e.what()
                   << " type: " << type_to_str(conn->type)
                   << " stream_id: " << rpc->get_ds_stream_id()
                   << " fd: " << rpc->get_ds_fd();
    }
}


void State::forward(ConnectionType type, ConnectionDirection direction) {
    if (type == ConnectionType::EGRESS && direction == ConnectionDirection::DOWNSTREAM) {
        // we don't route EGRESS DOWNSTREAM requests (handled by `ingress_admit` and `ppm_client`)
        return;
    }
    if (rpc_queue.empty(type, direction)) {
        return;
    }
    VLOG(3) << "Starting forwarding on type " << type_to_str(type) << " and direction " << direction_to_str(direction);

    // check if we have any RPC message in the queue
    while (!rpc_queue.empty(type, direction)) {
        auto [src_fd, src_stream_id] = rpc_queue.dequeue(type, direction);
        VLOG(3) << "Forwarding message (" << src_fd << "," << src_stream_id << ") of type " << type_to_str(type)
                   << " and direction " << direction_to_str(direction);

        if (direction == ConnectionDirection::DOWNSTREAM) {
            // we are dealing with a request (INGRESS-DOWNSTREAM)

            auto conn = route_request(type, src_stream_id, src_fd);
            auto rpc = rpc_mapper.get_ds_rpc(type, src_stream_id, src_fd);

            if (!forward_request(conn, rpc)) {
                break;
            }
            VLOG(1) << "RPCForward: INGRESS request for service: " << rpc->get_service();

            if (!config.is_ingress) {
                shared_state.ingress_request_admitted.add(rpc->get_service(), 1);
                uint32_t in_local = shared_state.ingress_request_admitted.get(rpc->get_service()) - shared_state.per_method_resp_in.get(rpc->get_service());
                utilization.update(in_local, rpc->get_service());
                if (in_local > local_state.local_concurrency_limit.get(rpc->get_service())) {
                    dump_entire_state();
                    LOG(FATAL) << in_local << " ingress requests in system for service: " << rpc->get_service()
                               << ", which is more than the configured limit: " << local_state.local_concurrency_limit.get(rpc->get_service());
                }
            }
        }
        else if (direction == ConnectionDirection::UPSTREAM) {
            // we are dealing with a response

            if (listeners.at(type)->no_connections()) {
                LOG(WARNING) << "No " << listeners.at(type)->type_to_str() << " connections available";
                VLOG(1) << "Finished routing on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
                return;
            }

            // get the RPC message
            auto rpc = rpc_mapper.get_us_rpc(type, src_stream_id, src_fd);
            int32_t ds_stream_id = rpc->get_ds_stream_id();
            int ds_fd = rpc->get_ds_fd();
            try {
                auto conn = listeners.at(type)->get_connection(rpc->get_ds_fd());

                // update stats
                if (!rpc->is_drop()) {
                    if (type == ConnectionType::EGRESS) {
                        local_state.ppm_client_dn_send.at(rpc->get_service()) = true;
                        local_state.egress_resp_in.add(rpc->get_service(), 1);
                        shared_state.downstream_concurrency.add(rpc->get_service(), -1);
                        // update the average service time
                        local_state.avg_service_time_us.get(rpc->get_service()).update(
                            (int32_t)std::chrono::duration_cast<std::chrono::microseconds>(
                                rpc->res_rcv_time - rpc->req_for_time
                            ).count()
                        );
                    } else if (type == ConnectionType::INGRESS) {
                        shared_state.per_method_resp_in.add(rpc->get_service(), 1);
                        uint32_t in_local = shared_state.ingress_request_admitted.get(rpc->get_service()) - shared_state.per_method_resp_in.get(rpc->get_service());
                        utilization.update(in_local, rpc->get_service());
                        check_credit_transmission(rpc->get_service(), false);
                    }

                    if (VLOG_IS_ON(1)) {
                        if (type == ConnectionType::EGRESS) {
                            VLOG(1) << "RPCForward: EGRESS response for service: " << rpc->get_service();
                        } else {
                            VLOG(1) << "RPCForward: INGRESS response for service: " << rpc->get_service();
                        }
                    }

                } else {
                    local_state.drops++;
                }

                // submit the response
                if (rpc->is_error()) {
                    if (rpc->http() == HTTP::HTTP1) {
                        conn->submit_error_response(std::move(rpc));
                    } else {
                        conn->submit_error_response(rpc);
                        rpc_mapper.remove_rpc(type, std::move(rpc));
                    }
                } else {
                    conn->submit_response(std::move(rpc));
                }
                write_http(conn);
                VLOG(3) << "Submitted response on stream " << ds_stream_id;


                
                
            } catch (const std::out_of_range& e) {
                dump_entire_state();
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

    VLOG(3) << "Finished forwarding on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
}

void State::remove_connection(std::shared_ptr<HTTPConnection> /*conn*/) {
    LOG(FATAL) << "Removing connection for upstream is not implemented";
    //pools.at(conn.type).remove_connection(conn.get_fd());
}

static std::string_view extract_service_from_ppm_req(const char* data) {
    size_t header_size = 5;
    if (data[1] != 0x01) {
        LOG(FATAL) << "Invalid message type";
    }
    if ((size_t)data[0] < header_size) {
        LOG(FATAL) << "Invalid message length: " << (int)data[0];
    }
    return std::string_view(data + header_size, (size_t)data[0] - header_size);
}

std::tuple<const std::string&, bool, size_t> State::valid_credit(const char* data) {
    // check the data format and extract the service name
    auto key = extract_service_from_ppm_req(data);

    // add the difference between requested credits and available credits to the denied requests
    int credit_diff = (int)(data[3] - data[4]);
    if (credit_diff > 0) {
        VLOG(1) << "PPMClient: Credit denied for service " << key
                << ", num denied requests: " << credit_diff
                << ", ppm_queue size: " << ppm_queue.size(ppm_queue.check(key));
    } else if (credit_diff < 0) {
        LOG(FATAL) << "Received more credits than requested: " << (int)data[3] << " vs " << (int)data[4];
    } else {
        VLOG(1) << "PPMClient: Valid credit for service " << key
            << ", new credits: " << (int)data[4]
            << ", ppm_queue size: " << ppm_queue.size(ppm_queue.check(key));
    }

    if (data[4] >= 1) {
        // make sure we are not receiving more than what we want
        return {ppm_queue.check(key), true, data[4]};
    } else {
        return {ppm_queue.check(key), false, 0};
    }
}

void State::ppm_client(bool dn_resp, const std::unique_ptr<Buffer>& dn_resp_buffer) {
    if (dn_resp) {
        // we have received a demand notification response
        auto [service, ok, num_credits] = valid_credit(dn_resp_buffer->data.data());
        if (!ok) {
            return;
        }

        if (num_credits > ppm_queue.size(service)) {
            dump_entire_state();
            LOG(FATAL) << "Received more credits than available in the queue for service: " << service
                       << ", num_credits: " << num_credits
                       << ", queue size: " << ppm_queue.size(service);
        }

        for (size_t i = 0; i < num_credits; i++) {
            auto rpc = ppm_queue.dequeue(service);
            forward_request(
                upstream_route_mapper.get_pool(service).get_connection(rpc->get_us_fd()),
                rpc
            );
            shared_state.downstream_concurrency.add(service, 1);
            local_state.ingress_admitted.add(service, 1);
            check_credit_transmission(rpc->get_service(), true);

            VLOG(1) << "RPCForward: EGRESS request for service: " << service
                    << ", admitted: " << local_state.ingress_admitted.get(service)
                    << ", ppm_queue size: " << ppm_queue.size(service);
        }
    } else {
        // we need to send a demand notification

        // first admit all requests to ppm queue
        size_t size = rpc_queue.size(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);
        for (size_t i = 0; i < size; i++) {
            auto [ds_fd, ds_stream_id] = rpc_queue.dequeue(
                ConnectionType::EGRESS,
                ConnectionDirection::DOWNSTREAM
            );
            route_request(ConnectionType::EGRESS, ds_stream_id, ds_fd);
            auto rpc = rpc_mapper.get_ds_rpc(
                ConnectionType::EGRESS,
                ds_stream_id,
                ds_fd
            );
            ppm_queue.enqueue(rpc);
            send_dn(
                    upstream_route_mapper.get_pool(rpc->get_service()).get_connection(ppm_queue.get_fd(rpc->get_service())).get(),
                    rpc->get_service(),
                    1
            );
            VLOG(1) << "PPMClient: DN for new request. "
                    << ", service: " << rpc->get_service()
                    << ", credits: " << 1
                    << ", queue size: " << ppm_queue.size(rpc->get_service());
        }
    }  
}

void State::send_dn(HTTPConnection* conn, const std::string& service, size_t num_credits) {
    // send a demand notification
    ssize_t header_size = 5;
    size_t len = (size_t)header_size + service.length();
    std::vector<char> msg(len);
    msg.at(0) = (char)len;
    msg.at(1) = 0x01; // demand notification (0x01)
    msg.at(2) = 0x00; // request (0x00), response (0x01)
    msg.at(3) = (char)num_credits; // number of requested credits
    // position 4 is for the received number of credits
    if (msg.size() - (size_t)header_size < service.length()) {
        LOG(FATAL) << "Buffer overflow"
                    << " , msg size: " << msg.size()
                    << " , header size: " << header_size
                    << " , service length: " << service.length();
    }
    std::copy_n(service.begin(), service.length(), msg.begin() + header_size);
    udp_send(msg, reinterpret_cast<struct sockaddr_in*>(conn->get_addr()));
}

void State::dump_entire_state() {
    LOG(INFO) << "Dumping entire state:";
    LOG(INFO) << "PPM State:";
    LOG(INFO) << "--- Sent Credits (shared_state.sent_credits) ---";
    for (auto& service : shared_state.sent_credits.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << shared_state.sent_credits.get(service);
    }
    LOG(INFO) << "--- (Upstream) Per Method Responses In (shared_state.per_method_resp_in) ---";
    for (auto& service : shared_state.per_method_resp_in.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << shared_state.per_method_resp_in.get(service);
    }
    LOG(INFO) << "--- Ingress Request Admitted (shared_state.ingress_request_admitted) ---";
    for (auto& service : shared_state.ingress_request_admitted.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << shared_state.ingress_request_admitted.get(service);
    }
    LOG(INFO) << "--- Downstream Concurrency (shared_state.downstream_concurrency) ---";
    for (auto& service : shared_state.downstream_concurrency.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << shared_state.downstream_concurrency.get(service);
    }
    LOG(INFO) << "--- PPM Client DN Send (local_state.ppm_client_dn_send) ---";
    for (const auto& [service, send] : local_state.ppm_client_dn_send) {
        LOG(INFO) << "  " << service << ": " << (send ? "true" : "false");
    }
    LOG(INFO) << "--- New PPM Queue Reqs (local_state.new_ppm_queue_reqs) ---";
    for (auto& service : local_state.new_ppm_queue_reqs.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << local_state.new_ppm_queue_reqs.get(service);
    }
    LOG(INFO) << "--- Ingress Queue Sizes (ingress) ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << ingress.size(route);
    }
    LOG(INFO) << "--- PPM Queue Sizes (ppm_queue) ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << ppm_queue.size(route);
    }
    LOG(INFO) << "--- RPC Queue Sizes (rpc_queue) ---";
    for (const auto& type : {ConnectionType::INGRESS, ConnectionType::EGRESS}) {
        for (const auto& direction : {ConnectionDirection::UPSTREAM, ConnectionDirection::DOWNSTREAM}) {
            LOG(INFO) << "  " << type_to_str(type) << " " << direction_to_str(direction)
                      << ": " << rpc_queue.size(type, direction);
        }
    }
    LOG(INFO) << "--- Ingress To Be Admitted (local_state.ingress_to_be_admitted) ---";
    for (auto& service : local_state.ingress_to_be_admitted.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << local_state.ingress_to_be_admitted.get(service);
    }
    LOG(INFO) << "--- Ingress Admitted (local_state.ingress_admitted) ---";
    for (auto& service : local_state.ingress_admitted.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << local_state.ingress_admitted.get(service);
    }
    LOG(INFO) << "---  Drops (local_state.drops) ---" << local_state.drops;
    LOG(INFO) << "---  Egress Responses In (local_state.egress_resp_in) ---";
    for (auto& service : local_state.egress_resp_in.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << local_state.egress_resp_in.get(service);
    }
    LOG(INFO) << "--- Average Service Time (local_state.avg_service_time_us) ---";
    for (auto& service : local_state.avg_service_time_us.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << local_state.avg_service_time_us.get(service).get_value();
    }
    LOG(INFO) << "--- Queueing Delay (ppm_queue.queueing_delay) ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << ppm_queue.queueing_delay(route);
    }
    LOG(INFO) << "--- extra slot ingress ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << local_state.ingress_limit.get(route) - local_state.ingress_to_be_admitted.get(route) + local_state.ingress_admitted.get(route);
    }
    LOG(INFO) << "--- extra slot system ---";
    for (const auto& [route, _] : config.routing) {
        LOG(INFO) << "  " << route << ": " << shared_state.downstream_concurrency.get(route) - local_state.ingress_limit.get(route);
    }
}

void State::ingress_admit() {
    // update ingress's p95 estimate
    if (std::chrono::steady_clock::now() >= next_hist_update && hist->total_count >= 500) {
        // FIX: the histogram is not updated correctly
        ingress.update_stats(
                (int32_t)hdr_value_at_percentile(hist, 50.0),
                (int32_t)hdr_value_at_percentile(hist, 95.0),
                ingress_service
        );
        VLOG(1) << "Average service time: " << local_state.avg_service_time_us.get(ingress_service).get_value()
                  << " for service: " << ingress_service;
        hdr_reset(hist);
        next_hist_update = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    }

    // check for any potential admitting or dropping
    int64_t current_queue_size = local_state.ingress_to_be_admitted.get(ingress_service)
                            - local_state.ingress_admitted.get(ingress_service);
    int64_t extra_slot_ingress = local_state.ingress_limit.get(ingress_service) - current_queue_size;

    bool extra_slot_system = shared_state.downstream_concurrency.get(ingress_service) - local_state.ingress_limit.get(ingress_service);
    int64_t admitted = ingress.add_to_be_admitted_or_drop(
        rpc_queue,
        rpc_mapper,
        ingress_service,
        extra_slot_ingress,
        ppm_queue.queueing_delay(ingress_service),
        extra_slot_system
    );
    if (ingress.size(ingress_service) != 0) {
        dump_entire_state();
        LOG(FATAL) << "Ingress queue size is not 0 for service: " << ingress_service;
    }
    local_state.ingress_to_be_admitted.add(ingress_service, admitted);

    // forward potential dropped requests
    forward(ConnectionType::EGRESS, ConnectionDirection::UPSTREAM);
    
}

inline static void write_dn_response(int result, const std::unique_ptr<Buffer>& req, const std::unique_ptr<Buffer>& resp) {
    // copy request to response
    if (resp->data.size() - resp->get_filled() < req->get_filled()) {
        LOG(FATAL) << "Buffer overflow"
                    << " , resp size: " << resp->data.size()
                    << " , filled: " << resp->get_filled()
                    << " , req size: " << req->get_filled();
    }
    std::copy_n(req->data.begin(), req->get_filled(), resp->data.begin());
    resp->data.at(2) = 0x01; // response
    resp->data.at(4) = (char)result;
    resp->set_filled(req->get_filled());
}

void State::check_credit_transmission(const std::string_view& in_service, bool is_dn) {
    if (is_dn && local_state.upstream_service.size() == 0) {
        VLOG(2) << "QM: No upstream service found";
        return;
    }

    const std::string_view& service = is_dn ? local_state.upstream_service.get(in_service) : in_service;

    if (local_state.failed_dn_info.get(service).size() == 0) {
        VLOG(2) << "QM: No failed DN info found for service: " << service;
        return;
    }

    if (int available_credits = get_available_credits(service); available_credits > 0) {
        uint32_t num_credits_sent = 0;
        while (available_credits > 0) {
            auto failed_dn_info = std::move(local_state.failed_dn_info.get(service).front());
            local_state.failed_dn_info.get(service).pop();

            if (available_credits >= failed_dn_info->num_rejected_requests) {
                // fully satisfy this unit
                send_credit(failed_dn_info->addr, service, failed_dn_info->num_rejected_requests);
                available_credits -= failed_dn_info->num_rejected_requests;
                num_credits_sent += (uint32_t)failed_dn_info->num_rejected_requests;
                VLOG(2) << "QM: Sent failed DN info (fully satisfied) for service: " << service
                        << ", number of credits: " << failed_dn_info->num_rejected_requests;
            } else {
                // partially satisfy this unit
                send_credit(failed_dn_info->addr, service, available_credits);
                failed_dn_info->num_rejected_requests -= available_credits;
                local_state.failed_dn_info.get(service).push(std::move(failed_dn_info));
                num_credits_sent += (uint32_t)available_credits;
                available_credits = 0;
                VLOG(2) << "QM: Sent failed DN info (partially satisfied) for service: " << service
                        << ", number of credits: " << available_credits
                        << ", remaining number of rejected requests: " << failed_dn_info->num_rejected_requests;
            }
            shared_state.sent_credits.add(service, num_credits_sent);
            num_credits_sent = 0;
            VLOG(2) << "QM: Failed DN info size for service: " << service << " is " << local_state.failed_dn_info.get(service).size();
            if (local_state.failed_dn_info.get(service).size() == 0) {
                break;
            }
        }
    }
}

int State::get_available_credits(const std::string_view& service) {
    int s_credits = (int)shared_state.sent_credits.get(service);
    int p_resp_in = (int)shared_state.per_method_resp_in.get(service);
    int downstreams = 0;

    // TODO: support parallel fan-out
    auto it = config.mapping.find(service);
    if (it == config.mapping.end()) {
        LOG(FATAL) << "Service not found in mapping: " << service;
    }
    for (const auto& ds_service : it->second.downstreams) {
        downstreams += shared_state.downstream_concurrency.get(ds_service);
    }
    //int available_credits = (config.ppm_limit + (int)p_resp_in + (int)c) - (int)s_credits;
    int available_credits = ((int)local_state.per_api_limit.get(service) + p_resp_in + downstreams) - s_credits;
    if (available_credits < 0) {
        available_credits = 0;
    }
    VLOG(2) << "QM: Available credits for service: " << service << " is " << available_credits
    << ", snet credits: " << s_credits
    << ", per method resp in: " << p_resp_in
    << ", downstream concurrency: " << downstreams
    << ", per api limit: " << local_state.per_api_limit.get(service);
    return available_credits;
}

void State::queue_multiplexer(const std::unique_ptr<Buffer>& req, const std::unique_ptr<Buffer>& resp) {
    // read the request
    if (req->data.at(1) == 0x01) {
        // we have demand notification

        // check if it's a request
        if (req->data.at(2) != 0x00) {
            LOG(FATAL) << "QM only handles DN requests";
        }

        char requested_credits = req->data.at(3);

        std::string_view service = extract_service_from_ppm_req(req->data.data());

        VLOG(2) << "QM: Received DN request for service: " << service;

        int available_credits = get_available_credits(service);
        uint8_t result = 0;
        uint32_t in_system = 0;
        for (const auto& loop_service : shared_state.per_method_resp_in.get_all_keys()) {
            in_system += shared_state.ingress_request_admitted.get(loop_service) - shared_state.per_method_resp_in.get(loop_service);
        }
        if (in_system >= local_state.local_concurrency_limit.get(service)) {
            result = 0;
        } else {
            if (available_credits >= (int)requested_credits) {
                result = (uint8_t)requested_credits;
            } else {
                // save address and number of rejected requests for future credit transmissions
                auto failed_dn_info = std::make_unique<FailedDNInfoUnit>(req->get_addr(), (int)requested_credits - available_credits);
                    VLOG(2) << "QM: Saving failed DN info for service: " << service
                            << ", new rejects: " << failed_dn_info->num_rejected_requests
                            << ", total reject units: " << local_state.failed_dn_info.get(service).size();
                    local_state.failed_dn_info.get(service).push(std::move(failed_dn_info));
                result = (uint8_t)available_credits;
            }
        }
        
        shared_state.sent_credits.add(service, result);

        // write the response
        write_dn_response(result, req, resp);
        if (VLOG_IS_ON(1)) {
            VLOG(1) << "QM: Wrote DN response for service: " << service
               << ", result: " << (int)result
               << ", requested: " << (int)requested_credits
                << ", available: " << available_credits
                << ", req in sys: " << in_system
               << ", sent credits: " << shared_state.sent_credits.get(service)
               << ", resp out: " << shared_state.per_method_resp_in.get(service);
        }
        
    } else {
        LOG(FATAL) << "Unknown message type";
    }
}

void State::udp_send(std::vector<char> msg, struct sockaddr_in* addr) {

    // write the message to a buffer
    auto buffer = buffer_manager.get_buffer();
    if (msg.size() > buffer->get_size()) {
        LOG(FATAL) << "Buffer overflow"
                    << " , msg size: " << msg.size()
                    << " , buffer size: " << buffer->get_size();
    }
    std::copy_n(msg.begin(), msg.size(), buffer->data.begin());
    buffer->set_filled(msg.size());
    
    // send the request using the ring
    ring.prepare_req_sendmsg(
        sockfd,
        std::move(buffer),
        buffer_manager.get_user_data(),
        *addr
    );
}

void State::send_credit(std::unique_ptr<struct sockaddr_in>& addr, const std::string_view& service, int num_credits) {
    ssize_t header_size = 5;
    size_t len = (size_t)header_size + service.length();
    std::vector<char> msg(len);
    msg.at(0) = (char)len;
    msg.at(1) = 0x01; // demand notification (0x01)
    msg.at(2) = 0x01; // request (0x00), response (0x01)
    msg.at(3) = (char)num_credits; // number of credits
    msg.at(4) = (char)num_credits; // number of credits
    if (msg.size() - (size_t)header_size < service.length()) {
        LOG(FATAL) << "Buffer overflow"
                    << " , msg size: " << msg.size()
                    << " , header size: " << header_size
                    << " , service length: " << service.length();
    }
    std::copy_n(service.begin(), service.length(), msg.begin() + header_size);
    udp_send(msg, addr.get());
}

SharedState::SharedState(std::vector<std::string> hosted_services, std::vector<std::string> downstream_services)
:   sent_credits(FastMap<uint32_t>(hosted_services)),
    per_method_resp_in(FastMap<uint32_t>(hosted_services)),
    ingress_request_admitted(FastMap<uint32_t>(hosted_services)),
    downstream_concurrency(FastMap<int64_t>(downstream_services))
{
}

static bool set_upstream_service(LocalMap<std::string>& upstream_service, const std::string& service) {
    bool found = false;
    for (const auto& [us, info] :config.mapping) {
        for (const auto& ds : info.downstreams) {
            if (ds == service) {
                upstream_service.set(service, us);
                if (found) {
                    LOG(FATAL) << "Multiple upstream services found for service: " << service;
                }
                found = true;
            }
        }
    }
    return found;
}

LocalState::LocalState(std::vector<std::string> hosted_services, std::vector<std::string> downstream_services)
:   local_concurrency_limit(LocalMap<uint32_t>(hosted_services)),
    per_api_limit(LocalMap<uint32_t>(hosted_services)),
    failed_dn_info(LocalMap<std::queue<std::unique_ptr<FailedDNInfoUnit>>>(hosted_services)),
    ppm_client_dn_send(std::unordered_map<std::string, bool>(downstream_services.size())),
    new_ppm_queue_reqs(LocalMap<uint32_t>(downstream_services)),
    upstream_service(LocalMap<std::string>(downstream_services)),
    egress_resp_in(LocalMap<uint32_t>(downstream_services)),
    drops(0),
    ingress_to_be_admitted(LocalMap<int64_t>(downstream_services)),
    ingress_admitted(LocalMap<int64_t>(downstream_services)),
    avg_service_time_us(LocalMap<MovingAverage>(downstream_services)),
    //last_admission(LocalMap<std::chrono::time_point<std::chrono::steady_clock>>(downstream_services)),
    ingress_limit(LocalMap<int32_t>(downstream_services))
{
        for (const auto& service : downstream_services) {
            ppm_client_dn_send.emplace(service, false);
            // TODO: If multiple hosted services map to the same downstream service, current implementation fails.
            // In that case, e need to track individual request IDs to to identify the correct upstream service.
            if (!set_upstream_service(upstream_service, service)) {
                LOG(FATAL) << "Upstream service not found for service: " << service;
            }
        }
        for (const auto& [service, info] : config.mapping) {
            per_api_limit.add(service, (uint32_t)info.limit);
        }
        for (const auto& [service, info] : config.routing) {
            ingress_limit.set(service, info.ingress_limit.value_or(0));
            LOG(INFO) << "Ingress limit for service: " << service << " is " << ingress_limit.get(service);
        }
    }