#include "state.h"
#include "buffer.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "connection_enums.h"
#include "fast_map.hpp"
#include "glog/logging.h"
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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <errno.h>
#include <exception>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(NANO_LOG_ENABLED) || defined(NABO_LOG_TRACE_ENABLED)
#include "NanoLog.h"
#include "NanoLogCpp17.h"

using namespace NanoLog::LogLevels;
#endif

UpstreamRouteMapper::UpstreamRouteMapper() : map() {}

void UpstreamRouteMapper::add_route(std::string service) {
  if (map.find(service) == map.end()) {
    map.emplace(service, ConnectionPool(ConnectionType::EGRESS));
  }
}

ConnectionPool &UpstreamRouteMapper::get_pool(const std::string &service) {
  if (map.find(service) == map.end()) {
    if (map.find("*") != map.end()) {
      return map.at("*");
    }
    // log keys
    for (const auto &[key, _] : map) {
      LOG(INFO) << "Available route: " << key;
    }
    LOG(FATAL) << "Service not found in routing table: " << service;
  } else {
    return map.at(service);
  }
}

State::State(Config parsed_config, RingWrapper &ring_ref,
             BufferManager &buffer_manager_ref, RPCMapper &mapper_ref,
             RPCQueue &queue_ref,
             std::unordered_map<ConnectionType, std::shared_ptr<Listener>>
                 &listeners_ref,
             Ingress &ingress_ref, SharedState &shared_state_ref,
             std::string &ingress_service_ref, int thread_id_arg,
             Stats &stats_ref)
    : config(parsed_config), ingress_pool(ConnectionType::INGRESS),
      upstream_route_mapper(), ring(ring_ref),
      buffer_manager(buffer_manager_ref), rpc_mapper(mapper_ref),
      rpc_queue(queue_ref), listeners(listeners_ref), ppm_queue(config.routing),
      ingress(ingress_ref), thread_id(thread_id_arg),
      shared_state(shared_state_ref),
      local_state(get_hosted_services(parsed_config),
                  get_downstream_services(parsed_config), ingress_service_ref),
      utilization(1000, get_hosted_services(parsed_config)),
      ingress_service(ingress_service_ref), stats(stats_ref) {

  if (config.buffer_size > HTTP1Connection_BUF_SIZE) {
    LOG(FATAL) << "Buffer size cannot be larger than "
               << HTTP1Connection_BUF_SIZE;
  }

  for (const auto &[route, info] : config.routing) {
    upstream_route_mapper.add_route(route);
    auto &pool = upstream_route_mapper.get_pool(route);
    int n_conn =
        config.is_ingress ? config.ingress_pool_connections.value() : 1;
    auto http_type = config.is_ingress ? HTTP::HTTP1 : HTTP::HTTP2;
    auto addr_list = name_resolver(info.upstream.host, info.upstream.port);

    // randomly shuffling the lift (avoid the thundering herd)
    auto rng = std::default_random_engine{};
    std::ranges::shuffle(addr_list, rng);

    for (auto &addr : addr_list) {
      auto &replica = pool.add_replica(addr);
      for (int i = 0; i < n_conn; i++) {
        auto conn = replica->add_connection(
            info.upstream.host, info.upstream.port, &rpc_mapper, &rpc_queue,
            http_type, &stats, &addr);

        // prepare connect
        ring.prepare_connect(std::move(conn), buffer_manager.get_user_data());
      }
    }
  }

  for (const auto &[service, info] : config.mapping) {
    /* local_state.local_concurrency_limit.set(service,
                                            (uint32_t)config.global_limit); */

    // for ingress, we should have the same name for hosted services and
    // downstream services
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
  global_limit connections for ingress requests. In the case of HTTP/2, we can
  easilly go up to 100 concurrent streams, but global_limit will limit the
  number concurrent streams.
  */
  int n_conn;
  if (config.is_frontend) {
    n_conn = config.frontend_pool_connections.value();
  } else if (!config.is_ingress) {
    n_conn = 1; // HTTP/2 connections can multiplex multiple streams
  }

  if (!config.is_ingress) {
    VLOG(2) << "Creating " << n_conn << " connections for INGRESS requests";
    auto ingress_host_list = name_resolver(config.ingress_upstream_host,
                                           config.ingress_upstream_port);
    if (ingress_host_list.size() != 1) {
      LOG(FATAL) << "got more than one address for INGRESS UPSREAM host";
    }
    auto ingress_replica_pool =
        ingress_pool.add_replica(ingress_host_list.at(0));
    for (int i = 0; i < n_conn; i++) {
      auto conn = ingress_replica_pool->add_connection(
          config.ingress_upstream_host, config.ingress_upstream_port,
          &rpc_mapper, &rpc_queue,
          (config.is_ingress || config.is_frontend) ? HTTP::HTTP1 : HTTP::HTTP2,
          &stats, &ingress_host_list.at(0));

      // prepare connect
      ring.prepare_connect(std::move(conn), buffer_manager.get_user_data());
    }
  }

  // RLP UDP: mesh binds well-known port + SO_REUSEPORT; ingress stays unbound
  // so credit replies return to the per-thread ephemeral source of each Credit
  // Request.
  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    LOG(FATAL) << "Failed to create socket";
  }
  if (!config.is_ingress) {
    int reuse = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) <
        0) {
      LOG(FATAL) << "Failed to set SO_REUSEPORT on RLP socket: "
                 << strerror(errno);
    }
    struct sockaddr_in rlp_addr{};
    rlp_addr.sin_family = AF_INET;
    rlp_addr.sin_port = htons(config.ingress_listener_port);
    rlp_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(sockfd, reinterpret_cast<struct sockaddr *>(&rlp_addr),
             sizeof(rlp_addr)) < 0) {
      LOG(FATAL) << "Failed to bind RLP UDP socket to port "
                 << config.ingress_listener_port << ": " << strerror(errno);
    }
  }

  LOG(INFO) << "global_limit: " << shared_state.credit_queue.get_global_limit();
  for (const auto &service : get_hosted_services(parsed_config)) {
    LOG(INFO) << "per_endpoint_limit for " << service << ": "
              << shared_state.credit_queue.get_per_endpoint_limit(service);
  }
}

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
      } catch (const BufferFullException &e) {
        // prepare existing buffer for write
        ring.prepare_write(conn, std::move(send_buffer),
                           buffer_manager.get_user_data());
        // get a new buffer
        send_buffer = buffer_manager.get_buffer();
        // write into new one
        if ((size_t)e.written >
            send_buffer->get_size() - send_buffer->get_filled()) {
          LOG(FATAL) << "Buffer too small for a single write, written: "
                     << e.written
                     << ", buffer size: " << send_buffer->get_size()
                     << ", filled: " << send_buffer->get_filled();
        }
        std::copy_n(e.outbuf_ptr, (size_t)e.written,
                    send_buffer->data.begin() +
                        (long)send_buffer->get_filled());
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
      ring.prepare_write(conn, std::move(send_buffer),
                         buffer_manager.get_user_data());
    }
  } else if (conn->http() == HTTP::HTTP1) {
    while (conn->want_write()) {
      auto send_buffer = buffer_manager.get_buffer();
      try {
        conn->http_write(send_buffer);
      } catch (const BufferFullException &e) {
        // NOTE: This should not happen (fix it like HTTP/2 if you have to)
        LOG(FATAL) << "Buffer full, written: " << e.written
                   << ", buffer size: " << send_buffer->get_size()
                   << ", filled: " << send_buffer->get_filled();
        break;
      }
      // conn->http_write(send_buffer);
      if (send_buffer->get_filled() == 0) {
        buffer_manager.free_buffer(std::move(send_buffer));
        break;
      }

      ring.prepare_write(conn, std::move(send_buffer),
                         buffer_manager.get_user_data());
    }
  } else {
    LOG(FATAL) << "Unknown HTTP type";
  }

  VLOG(3) << "Finished writing batch of HTTP data written on fd: "
          << conn->get_fd();
}

std::shared_ptr<HTTPConnection> State::route_request(ConnectionType type,
                                                     int32_t ds_stream_id,
                                                     int ds_fd,
                                                     RPCID credit_id) {
  // get the RPC message
  auto rpc = rpc_mapper.get_ds_rpc(type, ds_stream_id, ds_fd);
  std::optional<ConnectionPool *> pool;
  try {
    std::shared_ptr<HTTPConnection> conn;
    // TODO: implement load balancing within each connection pool
    if (type == ConnectionType::INGRESS) {
      conn = ingress_pool.lb()->get_any_conn();
      pool = std::nullopt;
    } else {
      pool.emplace(&upstream_route_mapper.get_pool(rpc->get_service()));
      conn = pool.value()->peek(credit_id);
    }

    if (conn->get_status() == ConnectionStatus::TEARDOWN) {
      LOG(WARNING)
          << "Connection is in TEARDOWN state. Starting a new connection.";
      throw NoConnectionException("Connection is in TEARDOWN state");
    } else if (conn->get_status() == ConnectionStatus::DOWN) {
      LOG(WARNING) << "Connection is in DOWN state.";
      // put the RPC message back in the queue
      rpc_queue.enqueue(type, ConnectionDirection::DOWNSTREAM, rpc->get_ds_fd(),
                        rpc->get_ds_stream_id());
      return nullptr;
    }

    // update RPC message metadata
    rpc->set_us_fd(conn->get_fd());

    // release the bind
    if (pool.has_value()) {
      pool.value()->release(credit_id);
    }

    VLOG(3) << "Routing request"
            << " of type: " << type_to_str(type)
            << " service: " << rpc->get_service() << " message (" << ds_fd
            << "," << ds_stream_id << ") to fd: " << conn->get_fd();

    return conn;
  } catch (NoConnectionException &e) {
    dump_entire_state();
    LOG(FATAL) << "No connection available for routing request: " << e.what()
               << " type: " << type_to_str(type)
               << " service: " << rpc->get_service();
  } catch (const std::exception &e) {
    LOG(FATAL) << "Error in routing, " << e.what();
  }
}

bool State::forward_request(std::shared_ptr<HTTPConnection> conn,
                            std::shared_ptr<RPCMessage> rpc) {
  // get an upstream connection
  try {

    if (conn->type == ConnectionType::INGRESS) {
      rpc->set_local_id_header();
    }

    // submit the request
    rpc->set_us_stream_id(conn->submit_request(rpc));

    // check if the request has an ID
    if (rpc->get_local_id() == -1 && !config.is_plain_frontend) {
      rpc->dump_req_headers();
      LOG(FATAL) << "Request has no ID (probably service does not provide IDs "
                    "or perhanps you should set is_plain_frontend to true)";
    }
    if (rpc->get_priority() == -1 && !config.is_plain_frontend) {
      rpc->dump_req_headers();
      LOG(FATAL)
          << "Request has no priority (probably service does not provide "
             "priorities or perhanps you should set is_plain_frontend "
             "to true)";
    }

    // update the mapping
    rpc_mapper.route(conn->type, rpc->get_ds_stream_id(), rpc->get_ds_fd(),
                     rpc->get_us_stream_id(), conn->get_fd());

    // flush the request
    write_http(conn);

    VLOG(3) << "Submitted request on stream " << rpc->get_us_stream_id();
    return true;
  } catch (NoConnectionException &e) {
    LOG(FATAL) << "No connection available. Starting a new connection.";
  } catch (std::exception &e) {
    LOG(FATAL) << "Error in forwarding request: " << e.what()
               << " type: " << type_to_str(conn->type)
               << " stream_id: " << rpc->get_ds_stream_id()
               << " fd: " << rpc->get_ds_fd();
  }
}

void State::forward(ConnectionType type, ConnectionDirection direction) {
  if (type == ConnectionType::EGRESS &&
      direction == ConnectionDirection::DOWNSTREAM) {
    // we don't route EGRESS DOWNSTREAM requests (handled by `Ingress::enqueue`
    // and `protocol_client`)
    return;
  }
  if (rpc_queue.empty(type, direction)) {
    return;
  }
  VLOG(3) << "Starting forwarding on type " << type_to_str(type)
          << " and direction " << direction_to_str(direction);

  // check if we have any RPC message in the queue
  while (!rpc_queue.empty(type, direction)) {
    auto [src_fd, src_stream_id] = rpc_queue.dequeue(type, direction);
    VLOG(3) << "Forwarding message (" << src_fd << "," << src_stream_id
            << ") of type " << type_to_str(type) << " and direction "
            << direction_to_str(direction);

    if (direction == ConnectionDirection::DOWNSTREAM) {
      // we are dealing with a request (INGRESS-DOWNSTREAM)

      auto conn = route_request(type, src_stream_id, src_fd, 0 /*placeholder*/);
      auto rpc = rpc_mapper.get_ds_rpc(type, src_stream_id, src_fd);

      if (!forward_request(conn, rpc)) {
        break;
      }
      VLOG(1) << "RPCForward: INGRESS request "
              << "| service: " << rpc->get_service()
              << "| id: " << rpc->get_id_string();

      if (!config.is_ingress) {
        shared_state.in_local.fetch_add(1);
        utilization.update((uint32_t)shared_state.in_local.load(),
                           rpc->get_service());
      }
    } else if (direction == ConnectionDirection::UPSTREAM) {
      // we are dealing with a response

      if (listeners.at(type)->no_connections()) {
        LOG(WARNING) << "No " << listeners.at(type)->type_to_str()
                     << " connections available";
        VLOG(1) << "Finished routing on type " << type_to_str(type)
                << " and direction " << direction_to_str(direction);
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
          if (rpc->get_local_id() == -1 && !config.is_plain_frontend) {
            LOG(FATAL)
                << "Response has no ID (probably service does not provide "
                   "IDs "
                   "or perhanps you should set is_plain_frontend to true)";
          }
          if (type == ConnectionType::EGRESS) {
            if (!config.is_ingress) {
              fanout_res_credit_management(rpc->get_local_id());
            }
            stats.time_mean_ds_concurrency.get(rpc->get_service()).down();
          } else if (type == ConnectionType::INGRESS) {
            // credit management and check for credit transmission
            shared_state.credit_queue.decrement_in_flight(rpc->get_service());
            check_credit_transmission();

            // update stats
            shared_state.in_local.fetch_sub(1);
            utilization.update((uint32_t)shared_state.in_local.load(),
                               rpc->get_service());
          }

          if (VLOG_IS_ON(1)) {
            if (type == ConnectionType::EGRESS) {
              VLOG(1) << "RPCForward: EGRESS response "
                      << "| service: " << rpc->get_service()
                      << "| id: " << rpc->get_id_string();
            } else {
              VLOG(1) << "RPCForward: INGRESS response "
                      << "| service: " << rpc->get_service()
                      << "| id: " << rpc->get_id_string();
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

      } catch (const std::out_of_range &e) {
        dump_entire_state();
        LOG(FATAL) << "No connection found for fd: " << ds_fd;
      } catch (const std::exception &e) {
        LOG(FATAL) << "Error in routing response: " << e.what()
                   << " type: " << type_to_str(type)
                   << " direction: " << direction_to_str(direction)
                   << " stream_id: " << src_stream_id << " fd: " << src_fd;
      }
    }
  }

  VLOG(3) << "Finished forwarding on type " << type_to_str(type)
          << " and direction " << direction_to_str(direction);
}

void State::remove_connection(std::shared_ptr<HTTPConnection> conn) {
  LOG(FATAL) << "Removing connection for upstream is not implemented. host: "
             << conn->get_host();
  // pools.at(conn.type).remove_connection(conn.get_fd());
}

static std::string_view extract_service_from_rlp_req(const char *data) {
  size_t header_size = 27;
  if (data[1] != 0x01 && data[1] != 0x02) {
    LOG(FATAL) << "Invalid message type: " << (int)data[1];
  }
  if ((size_t)data[0] < header_size) {
    LOG(FATAL) << "Invalid message length: " << (int)data[0];
  }
  return std::string_view(data + header_size, (size_t)data[0] - header_size);
}

std::tuple<const std::string &, bool, size_t, RPCID, FanOutType>
State::credit_post_process(const std::unique_ptr<Buffer> &buf) {

  const char *data = buf->data.data();

  // check the data format and extract the service name
  auto key = extract_service_from_rlp_req(data);
  const std::string &service =
      config.is_ingress ? ingress_service : ppm_queue.check(key);
  // extract the ID of the request (int64_t)
  RPCID id = (int64_t)((uint64_t)(unsigned char)data[5] << 56 |
                       (uint64_t)(unsigned char)data[6] << 48 |
                       (uint64_t)(unsigned char)data[7] << 40 |
                       (uint64_t)(unsigned char)data[8] << 32 |
                       (uint64_t)(unsigned char)data[9] << 24 |
                       (uint64_t)(unsigned char)data[10] << 16 |
                       (uint64_t)(unsigned char)data[11] << 8 |
                       (uint64_t)(unsigned char)data[12]);

  // update RTT
  int64_t timestamp =
      (int64_t)((uint64_t)(unsigned char)buf->data.at(15) << 56 |
                (uint64_t)(unsigned char)buf->data.at(16) << 48 |
                (uint64_t)(unsigned char)buf->data.at(17) << 40 |
                (uint64_t)(unsigned char)buf->data.at(18) << 32 |
                (uint64_t)(unsigned char)buf->data.at(19) << 24 |
                (uint64_t)(unsigned char)buf->data.at(20) << 16 |
                (uint64_t)(unsigned char)buf->data.at(21) << 8 |
                (uint64_t)(unsigned char)buf->data.at(22));
  int32_t queueing_time =
      (int32_t)((uint32_t)(unsigned char)buf->data.at(23) << 24 |
                (uint32_t)(unsigned char)buf->data.at(24) << 16 |
                (uint32_t)(unsigned char)buf->data.at(25) << 8 |
                (uint32_t)(unsigned char)buf->data.at(26));
  int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();
  int32_t total = (int32_t)(now_us - timestamp);
  int32_t rtt = total - queueing_time;
  if (rtt > 0) {
    local_state.last_rtt_us.set(service, rtt);
    stats.ema_ds_sidecar_rtt_us.get(service).update(rtt);
  } else {
    LOG(FATAL) << "Invalid RTT: " << rtt;
  }

  // add the difference between requested credits and available credits to the
  // denied requests
  int credit_diff = (int)(data[3] - data[4]);
  if (credit_diff != 0) {
    LOG(FATAL) << "Missmatch between credits requested: " << (int)data[3]
               << " vs " << (int)data[4];
  }
  if ((int)data[4] == 0) {
    LOG(FATAL) << "Receiving 0 credits is not allowed."
               << "service: " << key;
  }
  if (config.is_ingress && ingress_service != key) {
    LOG(FATAL) << "Received credits for a wrong service in ingress"
               << ", ingress_service: " << ingress_service
               << ", service of the credit: " << key;
  }

  VLOG(1) << "ProtocolClient: Valid credit "
          << "| id: " << id << "| service: " << key
          << "| new credits: " << (int)data[4]
          << "| ppm_queue size: " << ppm_queue.size(service);

  return {service, true, data[4], id, value_to_fanout_type(buf->data.at(14))};
}

void State::protocol_client(bool is_credit_grant,
                            const std::unique_ptr<Buffer> &credit_grant) {
  if (is_credit_grant) {
    // we have received a Credit Grant
    auto [service, ok, num_credits, credit_id, fanout_type] =
        credit_post_process(credit_grant);

    if (!ok) {
      return;
    }

    for (size_t i = 0; i < num_credits; i++) {
      if (!config.is_ingress) {
        fanout_req_management(credit_id, service, fanout_type, credit_grant);
      } else {
        LOG(FATAL)
            << "Ingress should not use protocol_client for sending requests "
               "post credit";
      }
    }
  } else {
    // we need to send a credit request

    // first admit all requests to ppm queue
    size_t size =
        rpc_queue.size(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);

    if (size > 0 && config.is_ingress) {
      LOG(FATAL) << "EGRESS-side requests in Ingress sidecar should go through "
                    "Ingress queue";
    }
    for (size_t i = 0; i < size; i++) {
      auto [ds_fd, ds_stream_id] = rpc_queue.dequeue(
          ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);
      auto rpc =
          rpc_mapper.get_ds_rpc(ConnectionType::EGRESS, ds_stream_id, ds_fd);
      rpc_mapper.register_id_map(rpc, ConnectionType::EGRESS);

      // send Credit Requests
      auto &ingress_rpc = rpc_mapper.get_ingress_rpc(rpc->get_local_id());
      auto &mapping_entry = config.mapping.at(ingress_rpc->get_service());
      FanOutType fanout_type;
      if (mapping_entry.dfanout.value_or(false)) {
        fanout_type = FanOutType::DYNAMIC;
      } else if (mapping_entry.pfanout.value_or(false)) {
        fanout_type = FanOutType::PARALELL;
      } else {
        fanout_type = FanOutType::SEQUENTIAL;
      }

      // for dynamic fan-out, send Credit Request to all downstreams
      if (mapping_entry.dfanout.value_or(false)) {
        ingress_rpc->dfanout_service = &rpc->get_service();
        for (auto &ds_service : mapping_entry.downstreams) {
          auto addr = upstream_route_mapper.get_pool(ds_service)
                          .acquire(rpc->get_local_id());
          send_credit_request(addr, ds_service, 1, rpc->get_local_id(),
                              rpc->get_priority(), fanout_type);
        }
      } else {
        auto addr = upstream_route_mapper.get_pool(rpc->get_service())
                        .acquire(rpc->get_local_id());
        send_credit_request(addr, rpc->get_service(), 1, rpc->get_local_id(),
                            rpc->get_priority(), fanout_type);
      }

      // we need to have the replica index before pushing
      int64_t queue_priority;
      if (config.mesh_late_binding &&
          config.late_binding_type == LateBindingType::EDF) {
        // Using EDF queue scheduling
        if (rpc->deadline == 0) {
          rpc->dump_req_headers();
          LOG(FATAL)
              << "Request has no deadline (probably service does not provide "
                 "priorities or perhanps you should set is_plain_frontend "
                 "to true)";
        }
        queue_priority = rpc->deadline;
      } else {
        // Using FCFS queue scheduling
        queue_priority = rpc->req_rcv_time.time_since_epoch().count();
      }
      ppm_queue.push(rpc->get_service(), rpc->get_local_id(), queue_priority);
    }
  }
}

/*
  This method is resonsible for credit management (sub request out event) and
  RPC transmission (for non-ingress) when downstream pattern is fanout
*/
void State::fanout_req_management(RPCID credit_id, const std::string &service,
                                  FanOutType fanout_type,
                                  const std::unique_ptr<Buffer> &credit_grant) {
  // credit management and transmission
  // Paralell and Dynamic only support early-binding
  if (fanout_type == FanOutType::PARALELL) {
    auto &ingress_rpc = rpc_mapper.get_ingress_rpc(credit_id);
    auto &mapping_entry = config.mapping.at(ingress_rpc->get_service());
    ingress_rpc->pfanout_req++;
    auto rpc_id = ppm_queue.pop(service, credit_id);
    auto rpc = rpc_mapper.get_egress_rpc(rpc_id);
    send_sub_request(std::move(rpc), credit_id);

    // if we are not the last branch, do nothing
    if (ingress_rpc->pfanout_req != mapping_entry.downstreams.size()) {
      return;
    }

    // for non pfanout or last branch in pfanout, decrement active requests
    shared_state.credit_queue.decrement_in_flight(ingress_rpc->get_service());
    check_credit_transmission();
  } else if (fanout_type == FanOutType::DYNAMIC) {
    auto &ingress_rpc = rpc_mapper.get_ingress_rpc(credit_id);
    auto &mapping_entry = config.mapping.at(ingress_rpc->get_service());
    ingress_rpc->pfanout_req++;
    // fill dfanout queue
    if (*ingress_rpc->dfanout_service != service) {
      auto credit_return = prepare_credit_return(credit_grant);
      credit_return->ret_service = &service;
      credit_return->ret_id = credit_id;
      ingress_rpc->credit_return_queue.push(std::move(credit_return));
      VLOG(2) << "ProtocolClient: Add credit for service: " << service
              << " credit_id: " << credit_id << " to Credit Return Queue";
    }

    if (ingress_rpc->pfanout_req != mapping_entry.downstreams.size()) {
      return;
    } else {
      auto rpc_id = ppm_queue.pop(*ingress_rpc->dfanout_service, credit_id);
      auto rpc = rpc_mapper.get_egress_rpc(rpc_id);
      send_sub_request(std::move(rpc), credit_id);

      // send out queued credit returns
      while (ingress_rpc->credit_return_queue.size() > 0) {
        auto ret = std::move(ingress_rpc->credit_return_queue.front());
        ingress_rpc->credit_return_queue.pop();

        // release bindings and stats
        upstream_route_mapper.get_pool(*ret->ret_service).release(ret->ret_id);

        ring.prepare_sendmsg(sockfd, std::move(ret),
                             buffer_manager.get_user_data());
      }
      VLOG(2) << "ProtocolClient: Flush Credit Return Queue for credit_id: "
              << credit_id;
    }

    // for non pfanout or last branch in pfanout, decrement active requests
    shared_state.credit_queue.decrement_in_flight(ingress_rpc->get_service());
    check_credit_transmission();
  } else {
    // sequential fanout
    auto rpc_id = config.mesh_late_binding ? ppm_queue.pop(service)
                                           : ppm_queue.pop(service, credit_id);
    auto rpc = rpc_mapper.get_egress_rpc(rpc_id);
    auto ingress_rpc = rpc_mapper.get_ingress_rpc(rpc->get_local_id());

    send_sub_request(std::move(rpc), credit_id);

    // for non pfanout or last branch in pfanout, decrement active requests
    shared_state.credit_queue.decrement_in_flight(ingress_rpc->get_service());
    check_credit_transmission();
  }
}

std::unique_ptr<Buffer>
State::prepare_credit_return(const std::unique_ptr<Buffer> &credit_grant) {
  auto credit_ret = buffer_manager.get_udp_buffer();

  // copy the content
  if (credit_ret->get_size() - credit_ret->get_filled() <
      credit_grant->get_filled()) {
    LOG(FATAL) << "buffer overflow";
  }
  std::copy_n(credit_grant->data.begin(), credit_grant->get_filled(),
              credit_ret->data.begin());
  credit_ret->set_filled(credit_grant->get_filled());

  // set the command code
  credit_ret->data.at(1) = 0x02; // credit return (0x02)

  // set the detination
  credit_ret->prepare_sendmsg(credit_grant->get_addr());

  return credit_ret;
}

void inline State::send_sub_request(std::shared_ptr<RPCMessage> rpc,
                                    RPCID credit_id) {
  /* if (ppm_queue.size(service) == 0) {
    dump_entire_state();
    LOG(FATAL)
        << "Received more credits than available in the queue for service: "
        << service << "| id: " << id
        << "| queue size: " << ppm_queue.size(service);
  }
  auto rpc = ppm_queue.pop(service, id); */
  auto &service = rpc->get_service();
  auto conn = route_request(ConnectionType::EGRESS, rpc->get_ds_stream_id(),
                            rpc->get_ds_fd(), credit_id);
  forward_request(conn, rpc);

  auto wd = (int32_t)std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - rpc->req_rcv_time)
                .count();
  stats.ema_wait_to_tx_us.get(service).update(wd);
  stats.time_mean_ds_concurrency.get(service).up();

  VLOG(1) << "RPCForward: EGRESS request. "
          << "| service: " << service << "| id: " << rpc->get_id_string()
          << "| ppm_queue size: " << ppm_queue.size(service);
}

/*
  This method is responsible for creditt management (sub response in event) when
  downstream pattern in fanout
*/
void State::fanout_res_credit_management(RPCID id) {
  auto &ingress_rpc = rpc_mapper.get_ingress_rpc(id);
  auto &entry = config.mapping.at(ingress_rpc->get_service());

  if (entry.pfanout.value_or(false)) {
    ingress_rpc->pfanout_res++;

    // if we are not the first branch, do nothing
    if (ingress_rpc->pfanout_res > 1) {
      return;
    }
  }

  // for non pfanout or first branch, increment active reuqests
  shared_state.credit_queue.increment_in_flight(ingress_rpc->get_service());
}

void State::send_credit_request(struct sockaddr_in addr,
                                const std::string &service, size_t num_credits,
                                RPCID id, Priority priority,
                                FanOutType fanout_type) {
  // send a Credit Request
  ssize_t header_size = 27;
  size_t len = (size_t)header_size + service.length();
  auto buffer = buffer_manager.get_udp_buffer();
  if (buffer->data.size() < len) {
    LOG(FATAL) << "Buffer size is too small";
  }
  buffer->data.at(0) = (char)len;
  buffer->data.at(1) = 0x01; // credit request (0x01)
  buffer->data.at(2) = 0x00; // Credit Request (0x00), Credit Grant (0x01)
  buffer->data.at(3) = (char)num_credits; // number of requested credits
  // position 4 is for the received number of credits
  // position 5 is for the ID of the request (int64_t - eight bytes)
  buffer->data.at(5) = (char)((unsigned char)(id >> 56));
  buffer->data.at(6) = (char)((unsigned char)(id >> 48));
  buffer->data.at(7) = (char)((unsigned char)(id >> 40));
  buffer->data.at(8) = (char)((unsigned char)(id >> 32));
  buffer->data.at(9) = (char)((unsigned char)(id >> 24));
  buffer->data.at(10) = (char)((unsigned char)(id >> 16));
  buffer->data.at(11) = (char)((unsigned char)(id >> 8));
  buffer->data.at(12) = (char)((unsigned char)(id & 0xFF));
  buffer->data.at(13) = (char)priority;
  buffer->data.at(14) = (char)fanout_type;
  int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();
  buffer->data.at(15) = (char)((unsigned char)(now_us >> 56));
  buffer->data.at(16) = (char)((unsigned char)(now_us >> 48));
  buffer->data.at(17) = (char)((unsigned char)(now_us >> 40));
  buffer->data.at(18) = (char)((unsigned char)(now_us >> 32));
  buffer->data.at(19) = (char)((unsigned char)(now_us >> 24));
  buffer->data.at(20) = (char)((unsigned char)(now_us >> 16));
  buffer->data.at(21) = (char)((unsigned char)(now_us >> 8));
  buffer->data.at(22) = (char)((unsigned char)(now_us & 0xFF));

  int32_t last_rtt = local_state.last_rtt_us.get(service);
  buffer->data.at(23) = (char)((unsigned char)(last_rtt >> 24));
  buffer->data.at(24) = (char)((unsigned char)(last_rtt >> 16));
  buffer->data.at(25) = (char)((unsigned char)(last_rtt >> 8));
  buffer->data.at(26) = (char)((unsigned char)(last_rtt & 0xFF));

  std::copy_n(service.begin(), service.length(),
              buffer->data.begin() + header_size);
  buffer->set_filled(len);
  ring.prepare_sendmsg_with_serveraddr(sockfd, std::move(buffer),
                                       buffer_manager.get_user_data(), addr);

  VLOG(1) << "ProtocolClient: Credit Request for new request "
          << "| service: " << service << "| id: " << id << "| credits: " << 1
          << "| queue size: " << ppm_queue.size(service);
}

void State::dump_entire_state() {
  LOG(INFO) << "Dumping entire state:";
  LOG(INFO) << "ingress_service: " << ingress_service;
  LOG(INFO) << "RLP State:";
  LOG(INFO) << "--- In Local "
               "(shared_state.in_local) ---";
  LOG(INFO) << "  " << shared_state.in_local.load();
  LOG(INFO) << "--- Ingress Queue Sizes (ingress) ---";
  LOG(INFO) << "  " << ingress.size();
  LOG(INFO) << "--- PPM Queue Sizes (ppm_queue) ---";
  for (const auto &[route, _] : config.routing) {
    LOG(INFO) << "  " << route << ": " << ppm_queue.size(route);
  }
  LOG(INFO) << "--- RPC Queue Sizes (rpc_queue) ---";
  for (const auto &type : {ConnectionType::INGRESS, ConnectionType::EGRESS}) {
    for (const auto &direction :
         {ConnectionDirection::UPSTREAM, ConnectionDirection::DOWNSTREAM}) {
      LOG(INFO) << "  " << type_to_str(type) << " "
                << direction_to_str(direction) << ": "
                << rpc_queue.size(type, direction);
    }
  }
  LOG(INFO) << "---  Drops (local_state.drops) ---" << local_state.drops;
}

void State::ingress_pre_credit() {
  if (stats.tail_e2e_time_us.get(ingress_service).consume_flush_updated()) {
    ingress.update_ingress_cap();
  }
  std::optional<std::tuple<RPCID, Priority>> potential;
  while ((potential = ingress.send_credit_request_checker()).has_value()) {
    // we should send a Credit Request
    auto [id, priority] = potential.value();
    auto addr = upstream_route_mapper.get_pool(ingress_service).acquire(id);
    send_credit_request(addr, ingress_service, 1, id, priority,
                        FanOutType::SEQUENTIAL);
  }
}

void State::ingress_post_credit(const std::unique_ptr<Buffer> &buf) {
  auto [service, ok, num_credits, credit_id, _] = credit_post_process(buf);

  if (!ok) {
    return;
  }

  if (num_credits != 1) {
    LOG(FATAL) << "we should only get 1 credit";
  }

  for (size_t i = 0; i < num_credits; i++) {
    // Ingress uses late-binding of credits to RPCs: Queue's head is used for
    // the new credit regardless of the credit_id
    auto rpc_optional = ingress.dequeue();
    if (rpc_optional.has_value()) {
      send_sub_request(std::move(rpc_optional.value()), credit_id);
    } else {
      // return the credit
      auto ret = prepare_credit_return(buf);
      ring.prepare_sendmsg(sockfd, std::move(ret),
                           buffer_manager.get_user_data());
      upstream_route_mapper.get_pool(service).release(credit_id);
      VLOG(1) << "Returned a credit due to lack of requests in Ingress queue";
    }
  }
  // drain drops
  forward(ConnectionType::EGRESS, ConnectionDirection::UPSTREAM);

  // send the next Credit Request
  ingress_pre_credit();
}

inline static void write_credit_grant(int granted_credits,
                                      const std::unique_ptr<Buffer> &req,
                                      const std::unique_ptr<Buffer> &grant) {
  // copy Credit Request to Credit Grant
  if (grant->data.size() - grant->get_filled() < req->get_filled()) {
    LOG(FATAL) << "Buffer overflow"
               << " , grant size: " << grant->data.size()
               << " , filled: " << grant->get_filled()
               << " , req size: " << req->get_filled();
  }
  std::copy_n(req->data.begin(), req->get_filled(), grant->data.begin());
  grant->data.at(2) = 0x01; // Credit Grant
  grant->data.at(4) = (char)granted_credits;
  grant->set_filled(req->get_filled());
}

inline static void
write_full_credit_grant(const std::unique_ptr<Buffer> &req,
                        const std::unique_ptr<Buffer> &resp) {
  write_credit_grant(1, req, resp);
  resp->prepare_sendmsg(req->get_addr());
}

void State::check_credit_transmission() {
  auto buffer = shared_state.credit_queue.pop();
  if (buffer == nullptr) {
    return;
  }

  // calculate the queuing time in credit_queue and set it on the Credit Grant
  int64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();
  int32_t queuing_time = (int32_t)(now_ts - buffer->enter_queue_ts);
  buffer->data.at(23) = (char)((unsigned char)(queuing_time >> 24));
  buffer->data.at(24) = (char)((unsigned char)(queuing_time >> 16));
  buffer->data.at(25) = (char)((unsigned char)(queuing_time >> 8));
  buffer->data.at(26) = (char)((unsigned char)(queuing_time & 0xFF));

  ring.prepare_sendmsg(sockfd, std::move(buffer),
                       buffer_manager.get_user_data());

  VLOG(1) << "ProtocolServer: Sent credit " << "| thread id: " << thread_id;
}

float State::cal_local_service_time(std::string_view us_service) {
  auto it = config.mapping.find(us_service);
  if (it == config.mapping.end()) {
    LOG(FATAL) << "service " << us_service << " should be in mapping.";
  }

  auto us_rt = stats.ema_us_service_time_us.get(us_service).get_value();

  if (it->second.pfanout.value_or(false)) {
    // find maximum service time
    auto max = 0.0F;
    for (auto &ds_service : it->second.downstreams) {
      auto value = stats.ema_ds_service_time_us.get(ds_service).get_value() +
                   stats.ema_wait_to_tx_us.get(ds_service).get_value();
      if (value > max) {
        max = value;
      }
    }

    return us_rt - max;
  } else if (it->second.dfanout.value_or(false)) {
    auto min = MAXFLOAT;
    for (auto &ds_service : it->second.downstreams) {
      auto value = stats.ema_ds_service_time_us.get(ds_service).get_value() +
                   stats.ema_wait_to_tx_us.get(ds_service).get_value();
      if (value < min) {
        min = value;
      }
    }

    return us_rt - min;
  } else {
    auto sum = 0.0F;
    for (auto &ds_service : it->second.downstreams) {
      sum += stats.ema_ds_service_time_us.get(ds_service).get_value() +
             stats.ema_wait_to_tx_us.get(ds_service).get_value();
    }

    return us_rt - sum;
  }
}

float State::get_theo_term(std::string_view service, bool sequential) {
  float theo_term = stats.ema_us_sidecar_rtt_us.get(service).get_value();
  if (sequential) {
    stats.ema_ds_sidecar_rtt_us.for_each_occupied_index([&](size_t i) {
      theo_term += stats.ema_ds_sidecar_rtt_us.by_index(i).get_value();
    });
  } else {
    float max_rtt = 0.0F;
    stats.ema_ds_sidecar_rtt_us.for_each_occupied_index([&](size_t i) {
      float v = stats.ema_ds_sidecar_rtt_us.by_index(i).get_value();
      if (v > max_rtt)
        max_rtt = v;
    });
    theo_term += max_rtt;
  }

  return theo_term;
}

void State::update_limits(int32_t rtt, std::string_view service) {
  VLOG(2) << "ProtocolServer: RTT " << rtt << " for service " << service;
  auto &rtt_stats = stats.ema_us_sidecar_rtt_us.get(service);
  rtt_stats.update(rtt);
  if (rtt_stats.get_count() % 2000 == 0) {
    VLOG(1) << "ProtocolServer: RTT " << rtt_stats.get_value()
            << " for service " << service;
    auto local_rt = cal_local_service_time(service);
    // auto local_rt = stats.ma_us_service_time_us.get(service).get_value();
    VLOG(1) << "ProtocolServer: Local Service time " << local_rt
            << " for service " << service;
    auto it = config.mapping.find(service);
    if (it == config.mapping.end()) {
      LOG(FATAL) << "service " << service << " not found in config.mapping";
    }
    bool sequential = !it->second.pfanout.value_or(false) &&
                      !it->second.dfanout.value_or(false);
    float theo_term = get_theo_term(service, sequential);
    int32_t new_limit =
        (std::max((int32_t)std::ceil(theo_term / local_rt), 1) + 1) *
        config.cpu_count.value();
    if (!it->second.dfanout.value_or(false)) {
      new_limit += it->second.downstreams.size();
    }
    new_limit += config.extra_limit;
    shared_state.credit_queue.update_endpoint_limit(new_limit, service);
    VLOG(1) << "ProtocolServer: New limit for service " << service << " is "
            << new_limit;

    // only update global limit for leaf services (intermediate services don't
    // use it)
    if (config.routing.size() == 0) {
      // update global_limit
      auto sum_limits = 0;
      auto max_limit = 0;
      for (auto &[us_service, _] : config.mapping) {
        auto limit =
            shared_state.credit_queue.get_per_endpoint_limit(us_service);
        sum_limits += limit;
        max_limit = limit > max_limit ? limit : max_limit;
      }

      shared_state.credit_queue.update_global_limit(
          max_limit + (int32_t)((float)(sum_limits - max_limit) *
                                config.over_commitment.value()));
    }

    VLOG(1) << "ProtocolServer: New global limit is "
            << shared_state.credit_queue.get_global_limit();
#ifdef NANO_LOG_ENABLED
    NANO_LOG(NOTICE, "M# %s LIMIT GLOBAL T:T %d", config.name.c_str(),
             shared_state.credit_queue.get_global_limit());
    NANO_LOG(NOTICE, "M# %s LIMIT LOCAL-%.*s T:T %d", config.name.c_str(),
             static_cast<int>(service.size()), service.data(),
             shared_state.credit_queue.get_per_endpoint_limit(service));
    NANO_LOG(NOTICE, "M# %s Measured Local-RT-%.*s T:T %d", config.name.c_str(),
             static_cast<int>(service.size()), service.data(),
             (int32_t)local_rt);
#endif
  }
}

void State::dispatch_rlp_recv(const std::unique_ptr<Buffer> &buf) {
  const size_t n = buf->get_filled();
  if (n < 3) {
    LOG(FATAL) << "RLP UDP payload too short: " << n;
  }
  const auto &d = buf->data;
  static constexpr size_t k_rlp_header = 27;
  size_t declared = (size_t)(unsigned char)d[0];
  if (declared < k_rlp_header || declared > n) {
    LOG(FATAL) << "Invalid RLP length byte: " << declared << " filled: " << n;
  }
  uint8_t b1 = (unsigned char)d[1];
  uint8_t b2 = (unsigned char)d[2];
  if (b1 == 0x02) {
    protocol_server(buf);
    return;
  }
  if (b1 == 0x01 && b2 == 0x00) {
    protocol_server(buf);
    return;
  }
  if (b1 == 0x01 && b2 == 0x01) {
    if (config.is_ingress) {
      ingress_post_credit(buf);
    } else {
      protocol_client(true, buf);
    }
    return;
  }
  LOG(FATAL) << "Unknown RLP UDP message: type " << (int)b1 << " flags "
             << (int)b2;
}

void State::protocol_server(const std::unique_ptr<Buffer> &req) {
  // read the request
  if (req->data.at(1) == 0x01) {
    // we have a credit request

    // check if it's a request
    if (req->data.at(2) != 0x00) {
      LOG(FATAL) << "ProtocolServer only handles Credit Requests";
    }

    char requested_credits = req->data.at(3);
    if (requested_credits != 1) {
      LOG(FATAL) << "Batching is not allowed";
    }

    std::string_view service = extract_service_from_rlp_req(req->data.data());
    RPCID rpc_id = (int64_t)((uint64_t)(unsigned char)req->data.at(5) << 56 |
                             (uint64_t)(unsigned char)req->data.at(6) << 48 |
                             (uint64_t)(unsigned char)req->data.at(7) << 40 |
                             (uint64_t)(unsigned char)req->data.at(8) << 32 |
                             (uint64_t)(unsigned char)req->data.at(9) << 24 |
                             (uint64_t)(unsigned char)req->data.at(10) << 16 |
                             (uint64_t)(unsigned char)req->data.at(11) << 8 |
                             (uint64_t)(unsigned char)req->data.at(12));
    Priority priority = (Priority)req->data.at(13);

    int32_t rtt = (int32_t)((uint32_t)(unsigned char)req->data.at(23) << 24 |
                            (uint32_t)(unsigned char)req->data.at(24) << 16 |
                            (uint32_t)(unsigned char)req->data.at(25) << 8 |
                            (uint32_t)(unsigned char)req->data.at(26));
    if (rtt >= 0) {
      update_limits((int32_t)rtt, service);
    } else {
      LOG(FATAL) << "Invalid RTT: " << rtt;
    }

    VLOG(1) << "ProtocolServer: Received Credit Request "
            << "| service: " << service << "| id: " << rpc_id
            << "| priority: " << priority << "| thread id: " << thread_id;

    auto resp = buffer_manager.get_udp_buffer();
    write_full_credit_grant(req, resp);
    resp->enter_queue_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    shared_state.credit_queue.push(std::move(resp), service, priority);

    // check credit transmission
    check_credit_transmission();

  } else if (req->data.at(1) == 0x02) {
    // credit return
    auto service = extract_service_from_rlp_req(req->data.data());
    VLOG(2) << "ProtocolServer: Received Credit Return "
            << "| service: " << service;
    shared_state.credit_queue.decrement_in_flight(service);
    check_credit_transmission();
  } else {
    LOG(FATAL) << "Unknown message type";
  }
}

SharedState::SharedState(std::vector<std::string> hosted_service,
                         std::vector<std::string> /*downstream_services*/)
    : credit_queue(hosted_service, config.cpu_count.value_or(-1)) {}

LocalState::LocalState(std::vector<std::string> /*hosted_services*/,
                       std::vector<std::string> downstream_services,
                       std::string & /*ingress_service*/)
    : drops(0), last_rtt_us(downstream_services) {}