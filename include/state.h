#pragma once

#include "buffer.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "connection_enums.h"
#include "credit_queue.hpp"
#include "fast_map.hpp"
#include "ingress.h"
#include "ppm_queue.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <unordered_map>
#include <vector>

class AddConnectionException : public std::runtime_error {
public:
  AddConnectionException(std::unique_ptr<HTTPConnection> &ex_conn)
      : std::runtime_error(""), conn(ex_conn) {}

  std::unique_ptr<HTTPConnection> &conn;
};

class ConnectionNotUPException : public std::runtime_error {
public:
  ConnectionNotUPException(std::unique_ptr<HTTPConnection> &ex_conn)
      : std::runtime_error(""), conn(ex_conn) {}

  std::unique_ptr<HTTPConnection> &conn;
};

class SharedState {
public:
  SharedState(std::vector<std::string>, std::vector<std::string>);

public:
  /*ConnectionType::INGRESS-side metrics*/

  std::atomic<int32_t> in_local;

  CreditQueue credit_queue;

  /*ConnectionType::EGRESS-side metrics*/
};

class LocalState {
public:
  LocalState(std::vector<std::string>, std::vector<std::string>, std::string &);

public:
  /*ConnectionType::INGRESS-side metrics*/

  /*ConnectionType::EGRESS-side metrics*/
  /*
  Average calculated waiting delay in the Ingress's queue
  */
  LocalMap<MovingAverage> ma_credit_delay_us;
  LocalMap<ExponentialMovingAverage> ema_ds_concurrency;
  LocalMap<TDigest> td_credit_delay_us;

  // number of drops (updated if only config.is_ingress is true)
  uint32_t drops;

  // TODO: in the future if we have multiple connections for the same service,
  // we should do this RTT measurement per connection.
  LocalMap<int32_t> last_rtt_us;
};

class UpstreamRouteMapper {
public:
  UpstreamRouteMapper();
  void add_route(std::string);
  ConnectionPool &get_pool(const std::string &);

private:
  std::unordered_map<std::string, ConnectionPool> map;
};

class State {

public:
  State(Config, RingWrapper &, BufferManager &, RPCMapper &, RPCQueue &,
        std::unordered_map<ConnectionType, std::shared_ptr<Listener>> &,
        Ingress &, SharedState &, std::string &, int);
  void forward(ConnectionType, ConnectionDirection);
  void remove_connection(std::shared_ptr<HTTPConnection>);

  // PPM-related functions
  void dispatch_ppm_recv(const std::unique_ptr<Buffer> &);
  void queue_multiplexer(const std::unique_ptr<Buffer> &);
  void ppm_client(bool, const std::unique_ptr<Buffer> &);
  void ingress_admit();

  /*Write request/response from connection's internal state to buffers.
  For HTTP/2 it also writes setting/ping/etc frames.*/
  void write_http(std::shared_ptr<HTTPConnection>);
  bool forward_request(std::shared_ptr<HTTPConnection>,
                       std::shared_ptr<RPCMessage>);
  std::shared_ptr<HTTPConnection> route_request(ConnectionType, int32_t, int);
  void dump_entire_state();
  int get_sockfd() { return sockfd; }

private:
  // PPM-related functions
  void send_dn(struct sockaddr_in, const std::string &, size_t, RPCID, Priority,
               bool);
  std::tuple<const std::string &, bool, size_t, RPCID>
  valid_credit(const char *);
  bool check_credit_available(std::string_view);
  void check_credit_transmission();
  void update_limits(int32_t, std::string_view);
  void fanout_req_management(RPCID, const std::string &);
  std::shared_ptr<RPCMessage> send_sub_request(RPCID, const std::string &);
  float cal_local_service_time(std::string_view);
  void fanout_res_credit_management(RPCID);

private:
  Config config;
  ConnectionPool ingress_pool;
  UpstreamRouteMapper upstream_route_mapper;
  RingWrapper &ring;
  BufferManager &buffer_manager;
  int sockfd; // UDP socket file descriptor
  RPCMapper &rpc_mapper;
  RPCQueue &rpc_queue;
  std::unordered_map<ConnectionType, std::shared_ptr<Listener>> &listeners;
  PPMQueue ppm_queue;
  Ingress &ingress;
  int thread_id;

public:
  SharedState &shared_state;
  LocalState local_state;
  Utilization utilization;
  std::string &ingress_service;
  Stats stats;
};