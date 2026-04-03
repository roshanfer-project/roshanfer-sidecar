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

std::vector<std::string> get_downstream_services(const Config &local_config) {
  std::vector<std::string> downstream_services;
  for (const auto &[route, _] : local_config.routing) {
    downstream_services.push_back(route);
  }
  return downstream_services;
}

std::vector<std::string> get_hosted_services(const Config &local_config) {
  std::vector<std::string> hosted_services;
  for (const auto &mapping : local_config.mapping) {
    hosted_services.push_back(mapping.first);
  }
  return hosted_services;
}

State::State(Config parsed_config, RingWrapper &ring_ref,
             BufferManager &buffer_manager_ref, RPCMapper &mapper_ref,
             RPCQueue &queue_ref,
             std::unordered_map<ConnectionType, std::shared_ptr<Listener>>
                 &listeners_ref,
             Ingress &ingress_ref, SharedState &shared_state_ref,
             std::string &ingress_service_ref, int thread_id_arg)
    : config(parsed_config), ingress_pool(ConnectionType::INGRESS),
      upstream_route_mapper(), ring(ring_ref),
      buffer_manager(buffer_manager_ref), rpc_mapper(mapper_ref),
      rpc_queue(queue_ref), listeners(listeners_ref), ppm_queue(config.routing),
      ingress(ingress_ref), thread_id(thread_id_arg),
      shared_state(shared_state_ref),
      local_state(get_hosted_services(parsed_config),
                  get_downstream_services(parsed_config), ingress_service_ref),
      utilization(1000, get_hosted_services(parsed_config)),
      ingress_service(ingress_service_ref),
      stats(get_downstream_services(parsed_config),
            get_hosted_services(parsed_config)) {

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
    for (int i = 0; i < n_conn; i++) {
      auto conn =
          pool.add_connection(info.upstream.host, info.upstream.port,
                              &rpc_mapper, &rpc_queue, http_type, &stats);

      // prepare connect
      ring.prepare_connect(std::move(conn), buffer_manager.get_user_data());
    }
  }

  for (const auto &[service, info] : config.mapping) {
    /* local_state.local_concurrency_limit.set(service,
                                            (uint32_t)config.ppm_limit); */

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
  for (int i = 0; i < n_conn; i++) {
    auto conn = ingress_pool.add_connection(
        config.ingress_upstream_host, config.ingress_upstream_port, &rpc_mapper,
        &rpc_queue,
        (config.is_ingress || config.is_frontend) ? HTTP::HTTP1 : HTTP::HTTP2,
        &stats);

    // prepare connect
    ring.prepare_connect(std::move(conn), buffer_manager.get_user_data());
  }

  // socket for UDP (Used for PPM)
  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    LOG(FATAL) << "Failed to create socket";
  }

  LOG(INFO) << "ppm_limit: " << shared_state.credit_queue.get_ppm_limit();
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

std::shared_ptr<HTTPConnection>
State::route_request(ConnectionType type, int32_t ds_stream_id, int ds_fd) {
  // get the RPC message
  auto rpc = rpc_mapper.get_ds_rpc(type, ds_stream_id, ds_fd);

  try {
    std::shared_ptr<HTTPConnection> conn;
    // TODO: implement load balancing within each connection pool
    if (type == ConnectionType::INGRESS) {
      conn = ingress_pool.get_any_connection();
    } else {
      conn = upstream_route_mapper.get_pool(rpc->get_service())
                 .get_any_connection();
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

    // submit the request
    rpc->set_us_stream_id(conn->submit_request(rpc));

    // check if the request has an ID
    if (rpc->get_id() == -1 && !config.is_plain_frontend) {
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
    // we don't route EGRESS DOWNSTREAM requests (handled by `ingress_admit`
    // and `ppm_client`)
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

      auto conn = route_request(type, src_stream_id, src_fd);
      auto rpc = rpc_mapper.get_ds_rpc(type, src_stream_id, src_fd);

      if (!forward_request(conn, rpc)) {
        break;
      }
      VLOG(1) << "RPCForward: INGRESS request "
              << "| service: " << rpc->get_service()
              << "| id: " << rpc->get_id();

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
          if (rpc->get_id() == -1 && !config.is_plain_frontend) {
            LOG(FATAL)
                << "Response has no ID (probably service does not provide "
                   "IDs "
                   "or perhanps you should set is_plain_frontend to true)";
          }
          if (type == ConnectionType::EGRESS) {
            if (!config.is_ingress) {
              fanout_res_credit_management(rpc->get_id());
            }
            local_state.ema_ds_concurrency.get(rpc->get_service()).down();
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
                      << "| id: " << rpc->get_id();
            } else {
              VLOG(1) << "RPCForward: INGRESS response "
                      << "| service: " << rpc->get_service()
                      << "| id: " << rpc->get_id();
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

void State::remove_connection(std::shared_ptr<HTTPConnection> /*conn*/) {
  LOG(FATAL) << "Removing connection for upstream is not implemented";
  // pools.at(conn.type).remove_connection(conn.get_fd());
}

static std::string_view extract_service_from_ppm_req(const char *data) {
  size_t header_size = 26;
  if (data[1] != 0x01) {
    LOG(FATAL) << "Invalid message type";
  }
  if ((size_t)data[0] < header_size) {
    LOG(FATAL) << "Invalid message length: " << (int)data[0];
  }
  return std::string_view(data + header_size, (size_t)data[0] - header_size);
}

std::tuple<const std::string &, bool, size_t, RPCID>
State::valid_credit(const char *data) {
  // check the data format and extract the service name
  auto key = extract_service_from_ppm_req(data);
  // extract the ID of the request (int64_t)
  RPCID id = (int64_t)((uint64_t)(unsigned char)data[5] << 56 |
                       (uint64_t)(unsigned char)data[6] << 48 |
                       (uint64_t)(unsigned char)data[7] << 40 |
                       (uint64_t)(unsigned char)data[8] << 32 |
                       (uint64_t)(unsigned char)data[9] << 24 |
                       (uint64_t)(unsigned char)data[10] << 16 |
                       (uint64_t)(unsigned char)data[11] << 8 |
                       (uint64_t)(unsigned char)data[12]);

  // add the difference between requested credits and available credits to the
  // denied requests
  int credit_diff = (int)(data[3] - data[4]);
  if (credit_diff > 0) {
    VLOG(1) << "PPMClient: Credit denied "
            << "| service: " << key << "| id: " << id
            << "| num denied requests: " << credit_diff
            << "| ppm_queue size: " << ppm_queue.size(ppm_queue.check(key));
  } else if (credit_diff < 0) {
    LOG(FATAL) << "Received more credits than requested: " << (int)data[3]
               << " vs " << (int)data[4];
  } else {
    VLOG(1) << "PPMClient: Valid credit "
            << "| id: " << id << "| service: " << key
            << "| new credits: " << (int)data[4]
            << "| ppm_queue size: " << ppm_queue.size(ppm_queue.check(key));
  }

  if (data[4] >= 1) {
    // make sure we are not receiving more than what we want
    return {ppm_queue.check(key), true, data[4], id};
  } else {
    return {ppm_queue.check(key), false, 0, id};
  }
}

void State::ppm_client(bool dn_resp,
                       const std::unique_ptr<Buffer> &dn_resp_buffer) {
  if (dn_resp) {
    // we have received a demand notification response
    auto [service, ok, num_credits, id] =
        valid_credit(dn_resp_buffer->data.data());

    // extract the timestamp
    int64_t timestamp =
        (int64_t)((uint64_t)(unsigned char)dn_resp_buffer->data.at(14) << 56 |
                  (uint64_t)(unsigned char)dn_resp_buffer->data.at(15) << 48 |
                  (uint64_t)(unsigned char)dn_resp_buffer->data.at(16) << 40 |
                  (uint64_t)(unsigned char)dn_resp_buffer->data.at(17) << 32 |
                  (uint64_t)(unsigned char)dn_resp_buffer->data.at(18) << 24 |
                  (uint64_t)(unsigned char)dn_resp_buffer->data.at(19) << 16 |
                  (uint64_t)(unsigned char)dn_resp_buffer->data.at(20) << 8 |
                  (uint64_t)(unsigned char)dn_resp_buffer->data.at(21));
    int32_t queueing_time =
        (int32_t)((uint32_t)(unsigned char)dn_resp_buffer->data.at(22) << 24 |
                  (uint32_t)(unsigned char)dn_resp_buffer->data.at(23) << 16 |
                  (uint32_t)(unsigned char)dn_resp_buffer->data.at(24) << 8 |
                  (uint32_t)(unsigned char)dn_resp_buffer->data.at(25));
    int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now().time_since_epoch())
                         .count();
    int32_t total = (int32_t)(now_us - timestamp);
    int32_t rtt = total - queueing_time;
    if (rtt > 0) {
      local_state.last_rtt_us.set(service, rtt);
    } else {
      LOG(FATAL) << "Invalid RTT: " << rtt;
    }

    if (!ok) {
      return;
    }

    if (num_credits > ppm_queue.size(service)) {
      dump_entire_state();
      LOG(FATAL)
          << "Received more credits than available in the queue for service: "
          << service << "| num_credits: " << num_credits << "| id: " << id
          << "| queue size: " << ppm_queue.size(service);
    }

    for (size_t i = 0; i < num_credits; i++) {
      auto rpc = ppm_queue.pop(service, id);
      route_request(ConnectionType::EGRESS, rpc->get_ds_stream_id(),
                    rpc->get_ds_fd());
      forward_request(upstream_route_mapper.get_pool(service).get_connection(
                          rpc->get_us_fd()),
                      rpc);
      if (!config.is_ingress) {
        fanout_req_credit_management(rpc->get_id());
      }
      auto wd = (int32_t)std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - rpc->req_rcv_time)
                    .count();
      local_state.ema_credit_delay_us.get(service).update(wd);
      local_state.td_credit_delay_us.add(wd);
      local_state.ema_ds_concurrency.get(service).up();
      VLOG(1) << "RPCForward: EGRESS request. "
              << "| service: " << service << "| id: " << rpc->get_id()
              << "| ppm_queue size: " << ppm_queue.size(service);
    }
  } else {
    // we need to send a demand notification

    // first admit all requests to ppm queue
    size_t size =
        rpc_queue.size(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);
    for (size_t i = 0; i < size; i++) {
      auto [ds_fd, ds_stream_id] = rpc_queue.dequeue(
          ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM);
      auto rpc =
          rpc_mapper.get_ds_rpc(ConnectionType::EGRESS, ds_stream_id, ds_fd);
      ppm_queue.push(rpc);
      send_dn(upstream_route_mapper.get_pool(rpc->get_service()).get_addr(),
              rpc->get_service(), 1, rpc->get_id(), rpc->get_priority());
      /* forward_request(upstream_route_mapper.get_pool(rpc->get_service())
                          .get_connection(rpc->get_us_fd()),
                      rpc);
      check_credit_transmission(); */
      VLOG(1) << "PPMClient: DN for new request "
              << "| service: " << rpc->get_service()
              << "| id: " << rpc->get_id() << "| credits: " << 1
              << "| queue size: " << ppm_queue.size(rpc->get_service());
    }
  }
}

void State::fanout_req_credit_management(RPCID id) {
  // credit management and transmission
  auto &ingress_rpc = rpc_mapper.get_ingress_rpc(id);
  bool pfanout =
      config.mapping.at(ingress_rpc->get_service()).pfanout.value_or(false);
  if (pfanout) {
    ingress_rpc->pfanout_req++;

    // if we are not the last branch, do nothing
    if (ingress_rpc->pfanout_req !=
        config.mapping.at(ingress_rpc->get_service()).downstreams.size()) {
      return;
    }
  }

  // for non pfanout or last branch in pfanout, decrement active requests
  shared_state.credit_queue.decrement_in_flight(ingress_rpc->get_service());
  check_credit_transmission();
}

void State::fanout_res_credit_management(RPCID id) {
  auto &ingress_rpc = rpc_mapper.get_ingress_rpc(id);

  if (config.mapping.at(ingress_rpc->get_service()).pfanout.value_or(false)) {
    ingress_rpc->pfanout_res++;

    // if we are not the first branch, do nothing
    if (ingress_rpc->pfanout_res > 1) {
      return;
    }
  }

  // for non pfanout or first branch, increment active reuqests
  shared_state.credit_queue.increment_in_flight(ingress_rpc->get_service());
}

void State::send_dn(struct sockaddr_in addr, const std::string &service,
                    size_t num_credits, RPCID id, Priority priority) {
  // send a demand notification
  ssize_t header_size = 26;
  size_t len = (size_t)header_size + service.length();
  auto buffer = buffer_manager.get_dn_buffer();
  if (buffer->data.size() < len) {
    LOG(FATAL) << "Buffer size is too small";
  }
  buffer->data.at(0) = (char)len;
  buffer->data.at(1) = 0x01;              // demand notification (0x01)
  buffer->data.at(2) = 0x00;              // request (0x00), response (0x01)
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
  int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();
  buffer->data.at(14) = (char)((unsigned char)(now_us >> 56));
  buffer->data.at(15) = (char)((unsigned char)(now_us >> 48));
  buffer->data.at(16) = (char)((unsigned char)(now_us >> 40));
  buffer->data.at(17) = (char)((unsigned char)(now_us >> 32));
  buffer->data.at(18) = (char)((unsigned char)(now_us >> 24));
  buffer->data.at(19) = (char)((unsigned char)(now_us >> 16));
  buffer->data.at(20) = (char)((unsigned char)(now_us >> 8));
  buffer->data.at(21) = (char)((unsigned char)(now_us & 0xFF));

  int32_t last_rtt = local_state.last_rtt_us.get(service);
  buffer->data.at(22) = (char)((unsigned char)(last_rtt >> 24));
  buffer->data.at(23) = (char)((unsigned char)(last_rtt >> 16));
  buffer->data.at(24) = (char)((unsigned char)(last_rtt >> 8));
  buffer->data.at(25) = (char)((unsigned char)(last_rtt & 0xFF));

  std::copy_n(service.begin(), service.length(),
              buffer->data.begin() + header_size);
  buffer->set_filled(len);
  ring.prepare_req_sendmsg(sockfd, std::move(buffer),
                           buffer_manager.get_user_data(), addr);
}

void State::dump_entire_state() {
  LOG(INFO) << "Dumping entire state:";
  LOG(INFO) << "ingress_service: " << ingress_service;
  LOG(INFO) << "PPM State:";
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

void State::ingress_admit() {
  if (ingress.size() == 0) {
    return;
  }
  if (ingress.size() != 1) {
    LOG(FATAL) << "Ingress queue size is not 1 (Ingress assumes single request "
                  "at a time)";
  }

  // check for any potential admitting or dropping
  int32_t queue_size = (int32_t)ppm_queue.size(ingress_service);
  int32_t wt =
      (int32_t)(stats.ema_ds_service_time_us.get(ingress_service).get_value() *
                (float)queue_size /
                local_state.ema_ds_concurrency.get(ingress_service)
                    .get_value_cap(1, INFINITY));

  int32_t e2e_delay =
      wt +
      (int32_t)stats.ema_ds_service_time_us.get(ingress_service).get_value();

  auto added =
      ingress.add_to_be_admitted_or_drop(rpc_queue, rpc_mapper, e2e_delay);
#ifdef NANO_LOG_ENABLED
  NANO_LOG(NOTICE, "M# %s Measured QS-%s T:T %d", config.name.c_str(),
           ingress_service.c_str(), queue_size);
  NANO_LOG(NOTICE, "M# %s Instant WT-%s T:T %d", config.name.c_str(),
           ingress_service.c_str(),
           (int32_t)ppm_queue.get_waiting_delay_us(ingress_service));
  NANO_LOG(NOTICE, "M# %s Estimated E2E-%s T:T %d", config.name.c_str(),
           ingress_service.c_str(), e2e_delay);
  if (added > 0) {
    NANO_LOG(NOTICE, "M# %s Estimated A-E2E-%s T:T %d", config.name.c_str(),
             ingress_service.c_str(), e2e_delay);
  }
  NANO_LOG(NOTICE, "M# %s Estimated RT-%s T:T %f", config.name.c_str(),
           ingress_service.c_str(),
           stats.ema_ds_service_time_us.get(ingress_service).get_value());
  NANO_LOG(NOTICE, "M# %s Estimated WT-R-%s T:T %d", config.name.c_str(),
           ingress_service.c_str(),
           (int32_t)(stats.tdigest_ds_service_time_us.get(ingress_service)
                         .get_quantile(0.95) *
                     (float)queue_size /
                     local_state.ema_ds_concurrency.get(ingress_service)
                         .get_value_cap(1, INFINITY)));
#endif
  if (ingress.size() != 0) {
    dump_entire_state();
    LOG(FATAL) << "Ingress queue size is not 0";
  }

  // forward potential dropped requests
  forward(ConnectionType::EGRESS, ConnectionDirection::UPSTREAM);
}

inline static void write_dn_response(int result,
                                     const std::unique_ptr<Buffer> &req,
                                     const std::unique_ptr<Buffer> &resp) {
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

inline static void
write_failed_dn_response(const std::unique_ptr<Buffer> &req,
                         const std::unique_ptr<Buffer> &resp) {
  write_dn_response(1, req, resp);
  resp->prepare_reply_sendmsg(req->get_addr());
}

void State::check_credit_transmission() {
  auto buffer = shared_state.credit_queue.pop();
  if (buffer == nullptr) {
    return;
  }

  // calculate the queuing time in credit_queue and set it to response
  int64_t now_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();
  int32_t queuing_time = (int32_t)(now_ts - buffer->enter_queue_ts);
  buffer->data.at(22) = (char)((unsigned char)(queuing_time >> 24));
  buffer->data.at(23) = (char)((unsigned char)(queuing_time >> 16));
  buffer->data.at(24) = (char)((unsigned char)(queuing_time >> 8));
  buffer->data.at(25) = (char)((unsigned char)(queuing_time & 0xFF));

  ring.prepare_reply_sendmsg(sockfd, std::move(buffer),
                             buffer_manager.get_user_data());

  VLOG(2) << "QM: Sent credit " << "| thread id: " << thread_id;
}

float State::cal_local_service_time(std::string_view us_service) {
  auto it = config.mapping.find(us_service);
  if (it == config.mapping.end()) {
    LOG(FATAL) << "service " << us_service << " should be in mapping.";
  }

  auto us_rt = stats.ma_us_service_time_us.get(us_service).get_value();

  if (it->second.pfanout.value_or(false)) {
    // find maximum service time
    auto max = 0.0F;
    for (auto &ds_service : it->second.downstreams) {
      auto value = stats.ema_ds_service_time_us.get(ds_service).get_value() +
                   local_state.ema_credit_delay_us.get(ds_service).get_value();
      if (value > max) {
        max = value;
      }
    }

    return us_rt - max;
  } else {
    auto sum = 0.0F;
    for (auto &ds_service : it->second.downstreams) {
      sum += stats.ema_ds_service_time_us.get(ds_service).get_value() +
             local_state.ema_credit_delay_us.get(ds_service).get_value();
    }

    return us_rt - sum;
  }
}

void State::update_limits(int32_t rtt, std::string_view service) {
  VLOG(2) << "QM: RTT " << rtt << " for service " << service;
  auto &rtt_stats = stats.ma_us_sidecar_rtt_us.get(service);
  rtt_stats.update(rtt);
  if (rtt_stats.get_count() % 2000 == 0) {
    VLOG(1) << "QM: RTT " << rtt_stats.get_value() << " for service "
            << service;
    auto local_rt = cal_local_service_time(service);
    // auto local_rt = stats.ma_us_service_time_us.get(service).get_value();
    VLOG(1) << "QM: Local Service time " << local_rt << " for service "
            << service;
    int32_t new_limit =
        (std::max((int32_t)std::ceil(rtt_stats.get_value() / local_rt), 1) +
         1) *
        config.cpu_count.value();
    new_limit += config.extra_limit;
    shared_state.credit_queue.update_endpoint_limit(new_limit, service);
    VLOG(1) << "QM: New limit for service " << service << " is " << new_limit;

    // update ppm_limit
    auto sum_limits = 0;
    auto max_limit = 0;
    for (auto &[us_service, _] : config.mapping) {
      auto limit = shared_state.credit_queue.get_per_endpoint_limit(us_service);
      sum_limits += limit;
      max_limit = limit > max_limit ? limit : max_limit;
    }
    shared_state.credit_queue.update_ppm_limit(
        max_limit + (int32_t)((float)(sum_limits - max_limit) *
                              config.over_commitment.value()));
    VLOG(1) << "QM: New ppm limit is "
            << shared_state.credit_queue.get_ppm_limit();
#ifdef NANO_LOG_ENABLED
    NANO_LOG(NOTICE, "M# %s LIMIT GLOBAL T:T %d", config.name.c_str(),
             shared_state.credit_queue.get_ppm_limit());
    NANO_LOG(NOTICE, "M# %s LIMIT LOCAL-%.*s T:T %d", config.name.c_str(),
             static_cast<int>(service.size()), service.data(),
             shared_state.credit_queue.get_per_endpoint_limit(service));
    NANO_LOG(NOTICE, "M# %s Measured Local-RT-%.*s T:T %d", config.name.c_str(),
             static_cast<int>(service.size()), service.data(),
             (int32_t)local_rt);
#endif
  }
}

void State::queue_multiplexer(const std::unique_ptr<Buffer> &req) {
  // read the request
  if (req->data.at(1) == 0x01) {
    // we have demand notification

    // check if it's a request
    if (req->data.at(2) != 0x00) {
      LOG(FATAL) << "QM only handles DN requests";
    }

    char requested_credits = req->data.at(3);
    if (requested_credits != 1) {
      LOG(FATAL) << "Batching is not allowed";
    }

    std::string_view service = extract_service_from_ppm_req(req->data.data());
    RPCID rpc_id = (int64_t)((uint64_t)(unsigned char)req->data.at(5) << 56 |
                             (uint64_t)(unsigned char)req->data.at(6) << 48 |
                             (uint64_t)(unsigned char)req->data.at(7) << 40 |
                             (uint64_t)(unsigned char)req->data.at(8) << 32 |
                             (uint64_t)(unsigned char)req->data.at(9) << 24 |
                             (uint64_t)(unsigned char)req->data.at(10) << 16 |
                             (uint64_t)(unsigned char)req->data.at(11) << 8 |
                             (uint64_t)(unsigned char)req->data.at(12));
    Priority priority = (Priority)req->data.at(13);

    int32_t rtt = (int32_t)((uint32_t)(unsigned char)req->data.at(22) << 24 |
                            (uint32_t)(unsigned char)req->data.at(23) << 16 |
                            (uint32_t)(unsigned char)req->data.at(24) << 8 |
                            (uint32_t)(unsigned char)req->data.at(25));
    if (rtt >= 0) {
      update_limits((int32_t)((float)rtt * 2), service);
    } else {
      LOG(FATAL) << "Invalid RTT: " << rtt;
    }

    VLOG(2) << "QM: Received DN request "
            << "| service: " << service << "| id: " << rpc_id
            << "| priority: " << priority << "| thread id: " << thread_id;

    auto resp = buffer_manager.get_dn_buffer();
    write_failed_dn_response(req, resp);
    resp->enter_queue_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    shared_state.credit_queue.push(std::move(resp), service, priority);

    // check credit transmission
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
                       std::string &ingress_service)
    : ema_credit_delay_us(downstream_services),
      ema_ds_concurrency(downstream_services), td_credit_delay_us(), drops(0),
      last_rtt_us(downstream_services) {
  for (auto &service : downstream_services) {
    ema_ds_concurrency.get(service).set_description("DSC-" + service);
    ema_credit_delay_us.get(service).set_description("Credit-Delay-" + service);
  }
}