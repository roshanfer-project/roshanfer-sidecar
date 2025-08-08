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
#include "snapshot.hpp"
#include "stats.h"
#include <algorithm>
#include <arpa/inet.h>
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
    Listener>& listeners_ref, Ingress& ingress_ref, SharedState& shared_state_ref, std::string& ingress_service_ref)
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
    shared_state(shared_state_ref),
    local_state(create_local_state(parsed_config)),
    utilization(create_utilization(parsed_config)),
    snapshot(Snapshot(4000, [this]() { this->dump_entire_state(); })),
    ingress_service(ingress_service_ref)
{

    if (config.buffer_size > HTTP1Connection_BUF_SIZE) {
        LOG(FATAL) << "Buffer size cannot be larger than " << HTTP1Connection_BUF_SIZE;
    }

    for (const auto& [route, info] : config.routing) {
        upstream_route_mapper.add_route(route);
        auto& pool = upstream_route_mapper.get_pool(route);
        int n_conn = config.is_ingress ? 70 : 1;
        auto http_type = config.is_ingress ? HTTP::HTTP1 : HTTP::HTTP2;
        for (int i = 0; i < n_conn; i++) {
            auto& conn = pool.add_connection(
                info.upstream.host,
                info.upstream.port,
                &rpc_mapper,
                &rpc_queue,
                http_type,
                hist
            );

            // prepare connect
            ring.prepare_connect(conn, buffer_manager.get_user_data());
        }
    }

    for (const auto& [service, info] : config.mapping) {
        local_state.local_concurrency_limit.set(service, (uint32_t)config.ppm_limit);
    }

    /* 
    Since HTTP/1.1 does not support multiplexing, we need to create at least
    ppm_limit connections for ingress requests. In the case of HTTP/2, we can
    easilly go up to 100 concurrent streams, but ppm_limit will limit the number 
    concurrent streams.
    */
    int n_conn;
    if (config.is_frontend) {
        n_conn = 50;
    } else if (config.is_ingress) {
        n_conn = 0;
    } else {
        n_conn = 1; // HTTP/2 connections can multiplex multiple streams
    }
    
    VLOG(2) << "Creating " << n_conn << " connections for ingress requests";
    for (int i = 0; i< n_conn; i++) {
        auto& conn = ingress_pool.add_connection(
            config.ingress_upstream_host,
            config.ingress_upstream_port,
            &rpc_mapper,
            &rpc_queue,
            (config.is_ingress || config.is_frontend) ? HTTP::HTTP1 : HTTP::HTTP2,
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

    if (conn->http() == HTTP::HTTP2) {
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
    } else if (conn->http() == HTTP::HTTP1) {
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
    } else {
        LOG(FATAL) << "Unknown HTTP type";
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

        VLOG(1) << "Routing request"
                << " of type: " << type_to_str(type)
                << " service: " << rpc->get_service()
                << " message (" << ds_fd << "," << ds_stream_id << ") to fd: " << conn->get_fd();

        return conn;
    }
    catch (NoConnectionException& e) {
        dump_entire_state();
        LOG(FATAL) << "No connection available for routing request: " << e.what()
                   << " type: " << type_to_str(type);
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
    } catch (std::exception& e) {
        LOG(FATAL) << "Error in forwarding request: " << e.what()
                   << " type: " << type_to_str(conn->type)
                   << " stream_id: " << rpc->get_ds_stream_id()
                   << " fd: " << rpc->get_ds_fd();
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
        VLOG(1) << "Forwarding message (" << src_fd << "," << src_stream_id << ") of type " << type_to_str(type)
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
            VLOG(1) << rpc->get_service() << " ---> LOCAL";

            if (!config.is_ingress) {
                shared_state.ingress_admitted.add(rpc->get_service(), 1);
                uint32_t in_local = shared_state.ingress_admitted.get(rpc->get_service()) - shared_state.per_method_resp_in.get(rpc->get_service());
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
                std::unique_ptr<HTTPConnection>& conn = listeners.at(type).get_connection(rpc->get_ds_fd());

                // update stats
                if (!rpc->is_drop()) {
                    if (type == ConnectionType::EGRESS) {
                        local_state.ppm_client_dn_send.at(rpc->get_service()) = true;
                        local_state.egress_resp_in.add(rpc->get_service(), 1);
                        shared_state.downstream_concurrency.add(rpc->get_service(), -1);
                    } else if (type == ConnectionType::INGRESS) {
                        shared_state.per_method_resp_in.add(rpc->get_service(), 1);
                        uint32_t in_local = shared_state.ingress_admitted.get(rpc->get_service()) - shared_state.per_method_resp_in.get(rpc->get_service());
                        utilization.update(in_local, rpc->get_service());
                    }

                    if (VLOG_IS_ON(1)) {
                        if (type == ConnectionType::EGRESS) {
                            VLOG(1) << rpc->get_service() << " --> LOCAL";
                        } else {
                            VLOG(1) << "LOCAL --> " << rpc->get_service();
                        }
                    }

                } else {
                    local_state.drops++;
                }

                // submit the response
                if (rpc->is_error()) {
                    conn->submit_error_response(*rpc);
                    if (rpc->http() == HTTP::HTTP2) {
                        rpc_mapper.remove_rpc(type, rpc);
                    }
                } else {
                    conn->submit_response(*rpc);
                }
                write_http(conn.get());
                VLOG(1) << "Submitted response on stream " << ds_stream_id;


                
                
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

    VLOG(1) << "Finished forwarding on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
}

void State::remove_connection(HTTPConnection& /*conn*/) {
    LOG(FATAL) << "Removing connection for upstream is not implemented";
    //pools.at(conn.type).remove_connection(conn.get_fd());
}

static std::string_view extract_service_from_ppm_req(const char* data) {
    if (data[1] != 0x01) {
        LOG(FATAL) << "Invalid message type";
    }
    if (data[0] < 5) {
        LOG(FATAL) << "Invalid message length: " << (int)data[0];
    }
    return std::string_view(data + 5, (size_t)data[0] - 5);
}

std::tuple<const std::string&, bool, size_t> State::valid_credit(const char* data) {
    // check the data format and extract the service name
    auto key = extract_service_from_ppm_req(data);

    // add the difference between requested credits and available credits to the denied requests
    int credit_diff = (int)(data[3] - data[4]);
    if (credit_diff > 0) {
        local_state.denied_reqs.add(key, (uint32_t)credit_diff);
        VLOG(1) << "PPMClient: Credit denied for service " << key
                << " with " << credit_diff << " additional credits"
                << ", total denied requests: " << local_state.denied_reqs.get(key);
    } else if (credit_diff < 0) {
        LOG(FATAL) << "Received more credits than requested: " << (int)data[3] << " vs " << (int)data[4];
    }

    if (data[4] >= 1) {
        return {ppm_queue.check(key), true, data[4]};
    } else {
        return {local_state.denied_reqs.get_key(key), false, 0};
    }
}

void State::ppm_client(bool dn_resp, Buffer* dn_resp_buffer) {
    if (dn_resp) {
        // we have received a demand notification response
        auto [service, ok, num_credits] = valid_credit(dn_resp_buffer->data.data());
        if (!ok) {
            // we have received a demand notification response but no credit
            VLOG(1) << "PPMClient: Credit denied for service " << service;
            return;
        }
        // we have received a credit
        VLOG(1) << "PPMClient: Credit received for service " << service
                << " with " << num_credits << " credits";

        if (num_credits > ppm_queue.size(service)) {
            dump_entire_state();
            LOG(FATAL) << "Received more credits than available in the queue for service: " << service
                       << ", num_credits: " << num_credits
                       << ", queue size: " << ppm_queue.size(service);
        }

        for (size_t i = 0; i < num_credits; i++) {
            auto rpc = ppm_queue.dequeue(service);
            forward_request(
                upstream_route_mapper.get_pool(service).get_connection(rpc->get_us_fd()).get(),
                rpc
            );
            shared_state.downstream_concurrency.add(service, 1);
            shared_state.ingress_transmitted.add(service, 1);
            VLOG(1) << "PPMClient: Forwarded request for service: " << service;
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

        for (auto& [service, send] : local_state.ppm_client_dn_send) {
            if (send) {
                if (ppm_queue.size(service) == 0) {
                    send = false;
                    continue;
                }
                auto num_denied = local_state.denied_reqs.get(service);
                if (num_denied == 0) {
                    send = false;
                    continue;
                }
                size_t requested_credits = 1;
                send_dn(
                    upstream_route_mapper.get_pool(service).get_connection(ppm_queue.get_fd(service)).get(),
                    service,
                    requested_credits
                );
                local_state.denied_reqs.set(service, num_denied-1);
                send = false; // reset the flag
                VLOG(1) << "PPMClient: Sent DN after new response for service: " << service
                        << ", credits: " << requested_credits
                        << ", denied requests: " << num_denied-1
                        << ", queue size: " << ppm_queue.size(service);
            }
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
    msg.at(3) = (char)num_credits; // number of credits
    // position 4 is for the received number of credits

    std::copy_n(service.begin(), service.length(), msg.begin() + header_size);
    udp_send(msg, reinterpret_cast<struct sockaddr_in*>(conn->get_addr()));
    //VLOG(1) << "PPMClient: Sent demand notification for service: " << service;
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
    LOG(INFO) << "--- Denied Requests (local_state.denied_reqs) ---";
    for (auto& service : local_state.denied_reqs.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << local_state.denied_reqs.get(service);
    }
    LOG(INFO) << "--- Ingress Admitted (shared_state.ingress_admitted) ---";
    for (auto& service : shared_state.ingress_admitted.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << shared_state.ingress_admitted.get(service);
    }
    LOG(INFO) << "--- Ingress Transmitted (shared_state.ingress_transmitted) ---";
    for (auto& service : shared_state.ingress_transmitted.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << shared_state.ingress_transmitted.get(service);
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
    LOG(INFO) << "---  Drops (local_state.drops) ---" << local_state.drops;
    LOG(INFO) << "---  Egress Responses In (local_state.egress_resp_in) ---";
    for (auto& service : local_state.egress_resp_in.get_all_keys()) {
        LOG(INFO) << "  " << service << ": " << local_state.egress_resp_in.get(service);
    }
}

void State::ingress_admit() {
    // update ingress's p95 estimate
    if (std::chrono::steady_clock::now() >= next_hist_update && hist->total_count >= 500) {
        ingress.update_stats(
                (int32_t)hdr_value_at_percentile(hist, 50.0),
                (int32_t)hdr_value_at_percentile(hist, 95.0),
                ingress_service
        );
        hdr_reset(hist);
        next_hist_update = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    }

    // check if we have any ingress requests for the service
    bool admit = false;
    uint32_t in_system = shared_state.ingress_admitted.get(ingress_service)
                        - local_state.egress_resp_in.get(ingress_service);
    uint32_t limit = (uint32_t)config.routing.at(ingress_service).ingress_limit.value();
    while (ingress.size(ingress_service) > 0) {
        if (limit > in_system) {
            admit = true;
            shared_state.ingress_admitted.add(ingress_service, 1);
            auto rpc = ingress.dequeue(ingress_service);
            rpc_queue.enqueue(
                ConnectionType::EGRESS,
                ConnectionDirection::DOWNSTREAM,
                rpc->get_ds_fd(),
                rpc->get_ds_stream_id()
            );
            VLOG(1) << "Admitted an ingress request for service " << ingress_service
                    << ", in system: " << in_system+1;
        } else {
            VLOG(1) << "Not admitted ingress request for service "
                    << ingress_service << " due to limit: " << limit
                    << ", admitted: " << shared_state.ingress_admitted.get(ingress_service)
                    << ", in system: " << in_system
                    << ", queue size: " << ingress.size(ingress_service);
            break;
        }
    }

    if (admit) {
        forward(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);
    }

    // check for any potential dropping
    if (ingress.check_drop(rpc_queue, rpc_mapper, ingress_service, limit)) {
        forward(ConnectionType::EGRESS, ConnectionDirection::UPSTREAM);
    }
    
}

inline static void write_dn_response(int result, Buffer* req, Buffer* resp) {
    // copy request to response
    std::copy_n(req->data.begin(), req->get_filled(), resp->data.begin());
    resp->data.at(2) = 0x01; // response
    resp->data.at(4) = (char)result;
    resp->set_filled(req->get_filled());
}

void State::queue_multiplexer(Buffer* req, Buffer* resp) {
    // read the request
    if (req->data.at(1) == 0x01) {
        // we have demand notification

        // check if it's a request
        if (req->data.at(2) != 0x00) {
            LOG(FATAL) << "QM only handles DN requests";
        }

        char requested_credits = req->data.at(3);

        std::string_view service = extract_service_from_ppm_req(req->data.data());

        // produce the response
        auto s_credits = shared_state.sent_credits.get(service);
        auto p_resp_in = shared_state.per_method_resp_in.get(service);
        //int available_credits = (config.ppm_limit + (int)p_resp_in + (int)c) - (int)s_credits;
        int available_credits = ((int)local_state.per_api_limit.get(service) + (int)p_resp_in) - (int)s_credits;
        if (available_credits < 0) {
            available_credits = 0;
        }
        uint8_t result = 0;
        uint32_t in_system = 0;
        for (const auto& loop_service : shared_state.per_method_resp_in.get_all_keys()) {
            in_system += shared_state.ingress_admitted.get(loop_service) - shared_state.per_method_resp_in.get(loop_service);
        }
        if (in_system >= local_state.local_concurrency_limit.get(service)) {
            result = 0;
        } else {
            if (available_credits >= (int)requested_credits) {
                result = (uint8_t)requested_credits;
            } else {
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
                << ", req in sys: " << shared_state.per_method_resp_in.get(service) - p_resp_in
               << ", sent credits: " << shared_state.sent_credits.get(service)
               << ", resp out: " << p_resp_in;
        }
        
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

SharedState::SharedState(std::vector<std::string> hosted_services, std::vector<std::string> downstream_services)
:   sent_credits(FastMap<uint32_t>(hosted_services)),
    per_method_resp_in(FastMap<uint32_t>(hosted_services)),
    ingress_admitted(FastMap<uint32_t>(hosted_services)),
    downstream_concurrency(FastMap<int64_t>(downstream_services)),
    ingress_transmitted(FastMap<uint32_t>(downstream_services))
{}

LocalState::LocalState(std::vector<std::string> hosted_services, std::vector<std::string> downstream_services)
:   local_concurrency_limit(LocalMap<uint32_t>(hosted_services)),
    per_api_limit(LocalMap<uint32_t>(hosted_services)),
    denied_reqs(LocalMap<uint32_t>(downstream_services)),
    ppm_client_dn_send(std::unordered_map<std::string, bool>(downstream_services.size())),
    new_ppm_queue_reqs(LocalMap<uint32_t>(downstream_services)),
    egress_resp_in(LocalMap<uint32_t>(downstream_services)),
    drops(0) {
        for (const auto& service : downstream_services) {
            ppm_client_dn_send.emplace(service, false);
        }
        for (const auto& [service, info] : config.mapping) {
            per_api_limit.add(service, (uint32_t)info.limit);
        }
    }