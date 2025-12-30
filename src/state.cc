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
      ingress(ingress_ref), hist(), thread_id(thread_id_arg),
      shared_state(shared_state_ref),
      local_state(get_hosted_services(parsed_config),
                  get_downstream_services(parsed_config)),
      utilization(1000, get_hosted_services(parsed_config)),
      ingress_service(ingress_service_ref),
      stats(get_downstream_services(
          parsed_config)) // Note that only ingress uses this list of services
                          // (so it should be EGRESS-side)
{

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

  if (int ret = hdr_init(1, 5000000, 3, &hist); ret < 0) {
    LOG(FATAL) << "Failed to initialize histogram: " << strerror(ret);
  }
  stats.update_hist(hist);
  for (const auto &service : get_downstream_services(parsed_config)) {
    stats.get_avg_service_time_us().get(service).set_description("RT-" +
                                                                 service);
  }
  LOG(INFO) << "State initialized";
  next_hist_update = std::chrono::steady_clock::now() + std::chrono::seconds(1);
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
            shared_state.credit_queue.increment_in_flight(rpc->get_service());
          } else if (type == ConnectionType::INGRESS) {
            shared_state.credit_queue.decrement_in_flight(rpc->get_service());
            shared_state.in_local.fetch_sub(1);
            utilization.update((uint32_t)shared_state.in_local.load(),
                               rpc->get_service());
            check_credit_transmission();
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
  size_t header_size = 9;
  if (data[1] != 0x01) {
    LOG(FATAL) << "Invalid message type";
  }
  if ((size_t)data[0] < header_size) {
    LOG(FATAL) << "Invalid message length: " << (int)data[0];
  }
  return std::string_view(data + header_size, (size_t)data[0] - header_size);
}

std::tuple<const std::string &, bool, size_t, int32_t>
State::valid_credit(const char *data) {
  // check the data format and extract the service name
  auto key = extract_service_from_ppm_req(data);
  // extract the ID of the request (int32_t)
  int32_t id =
      (int32_t)((unsigned char)data[5] << 24 | (unsigned char)data[6] << 16 |
                (unsigned char)data[7] << 8 | (unsigned char)data[8]);

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
      shared_state.credit_queue.decrement_in_flight(service);
      check_credit_transmission();
      local_state.avg_cal_waiting_delay.update(
          (int32_t)std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - rpc->req_rcv_time)
              .count());

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
              rpc->get_service(), 1, rpc->get_id());
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

void State::send_dn(struct sockaddr_in addr, const std::string &service,
                    size_t num_credits, int32_t id) {
  // send a demand notification
  ssize_t header_size = 9;
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
  // position 5 is for the ID of the request (int32_t - four bytes)
  buffer->data.at(5) = (char)((unsigned char)(id >> 24));
  buffer->data.at(6) = (char)((unsigned char)(id >> 16));
  buffer->data.at(7) = (char)((unsigned char)(id >> 8));
  buffer->data.at(8) = (char)((unsigned char)(id & 0xFF));
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
  // update ingress's p95 estimate
  if (std::chrono::steady_clock::now() >= next_hist_update &&
      hist->total_count >= 500) {
    // FIX: the histogram is not updated correctly
    ingress.update_stats((int32_t)hdr_value_at_percentile(hist, 50.0),
                         (int32_t)hdr_value_at_percentile(hist, 95.0));
    hdr_reset(hist);
    next_hist_update =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  }

  // check for any potential admitting or dropping
  int32_t queue_size = (int32_t)ppm_queue.size(ingress_service);
  int32_t queueing_delay =
      (int32_t)local_state.avg_cal_waiting_delay.get_value() * queue_size +
      (int32_t)stats.get_tdigest(ingress_service).get_quantile(0.95);

  auto added =
      ingress.add_to_be_admitted_or_drop(rpc_queue, rpc_mapper, queueing_delay);
  if (added > 0) {
#ifdef NANO_LOG_ENABLED
    NANO_LOG(NOTICE, "M# %s Measured QS-%s T:T %ld", config.name.c_str(),
             ingress_service.c_str(), queue_size);
    NANO_LOG(NOTICE, "M# %s Custom WT-%s T:T %d", config.name.c_str(),
             ingress_service.c_str(),
             (int32_t)local_state.avg_cal_waiting_delay.get_value() *
                 queue_size);
    NANO_LOG(NOTICE, "M# %s Custom QD-%s T:T %d", config.name.c_str(),
             ingress_service.c_str(), queueing_delay);
    NANO_LOG(NOTICE, "M# %s Custom TD-P95-%s T:T %f", config.name.c_str(),
             ingress_service.c_str(),
             stats.get_tdigest(ingress_service).get_quantile(0.95));
#endif
  }
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
  ring.prepare_reply_sendmsg(sockfd, std::move(buffer),
                             buffer_manager.get_user_data());

  VLOG(2) << "QM: Sent credit " << "| thread id: " << thread_id;
}

bool State::check_credit_available(std::string_view api) {
  return shared_state.credit_queue.check_credit_available(api);
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
    int32_t rpc_id = (int32_t)((unsigned char)req->data.at(5) << 24 |
                               (unsigned char)req->data.at(6) << 16 |
                               (unsigned char)req->data.at(7) << 8 |
                               (unsigned char)req->data.at(8));
    VLOG(2) << "QM: Received DN request "
            << "| service: " << service << "| id: " << rpc_id
            << "| thread id: " << thread_id;

    bool credits_available = check_credit_available(service);
    uint8_t result = 0;
    if (credits_available) {
      result = 1;
    }

    if (result == 0) {
      // save address and number of rejected requests for future credit
      // transmissions
      stats.mode2_credits.up(1);
      auto resp = buffer_manager.get_dn_buffer();
      write_failed_dn_response(req, resp);
      shared_state.credit_queue.push(std::move(resp), service);
      VLOG(2) << "QM: Saving failed DN info "
              << "| service: " << service << "| id: " << rpc_id
              << "| thread id: " << thread_id;
    } else {

      // check failed DN info
      check_credit_transmission();

      // write the response
      auto resp = buffer_manager.get_dn_buffer();
      write_dn_response(result, req, resp);
      ring.prepare_reply_sendmsg(sockfd, req, std::move(resp),
                                 buffer_manager.get_user_data());
      if (VLOG_IS_ON(1)) {
        VLOG(1) << "QM: Wrote DN response "
                << "| id: " << rpc_id << "| service: " << service
                << "| result: " << (int)result
                << "| requested: " << (int)requested_credits
                << "| in_local: " << shared_state.in_local.load()
                << "| thread id: " << thread_id;
      }
    }

  } else {
    LOG(FATAL) << "Unknown message type";
  }
}

SharedState::SharedState(std::vector<std::string> hosted_service,
                         std::vector<std::string> /*downstream_services*/)
    : credit_queue(hosted_service, config.ppm_limit.value_or(-1),
                   config.per_endpoint_limit.value_or(-1)) {}

LocalState::LocalState(std::vector<std::string> /*hosted_services*/
                       ,
                       std::vector<std::string> /*downstream_services*/)
    : avg_cal_waiting_delay(), drops(0) {}