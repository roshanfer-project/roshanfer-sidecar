#pragma once

#include "buffer.hpp"
#include "buffer_manager.hpp"
#include "config.hpp"
#include "connection.hpp"
#include "connection_enums.hpp"
#include "credit_queue.hpp"
#include "fast_map.hpp"
#include "ingress.hpp"
#include "ppm_queue.hpp"
#include "ring_wrapper.hpp"
#include "rpc_mapper.hpp"
#include "rpc_message.hpp"
#include "rpc_queue.hpp"
#include "stats.hpp"
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
  SharedState(std::vector<std::string>, const std::vector<std::string>&);

public:
  /*ConnectionType::INGRESS-side metrics*/

  std::atomic<int32_t> in_local;

  CreditQueue credit_queue;

  /*ConnectionType::EGRESS-side metrics*/
};

class LocalState {
public:
  LocalState(const std::vector<std::string>&, const std::vector<std::string>&, std::string &);

public:
  /*ConnectionType::INGRESS-side metrics*/

  /*ConnectionType::EGRESS-side metrics*/

  // number of drops (updated if only config.is_ingress is true)
  uint32_t drops{0};

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
  State(const Config&, RingWrapper &, BufferManager &, RPCMapper &, RPCQueue &,
        std::unordered_map<ConnectionType, std::shared_ptr<Listener>> &,
        Ingress &, SharedState &, std::string &, int, Stats &);
  void forward(ConnectionType, ConnectionDirection);
  void remove_connection(const std::shared_ptr<HTTPConnection>&);

  // PPM-related functions
  void dispatch_ppm_recv(const std::unique_ptr<Buffer> &);
  void queue_multiplexer(const std::unique_ptr<Buffer> &);
  void ppm_client(bool, const std::unique_ptr<Buffer> &);

  void ingress_pre_credit();
  void ingress_post_credit(const std::unique_ptr<Buffer> &);

  /*Write request/response from connection's internal state to buffers.
  For HTTP/2 it also writes setting/ping/etc frames.*/
  void write_http(const std::shared_ptr<HTTPConnection>&);
  bool forward_request(const std::shared_ptr<HTTPConnection>&,
                       const std::shared_ptr<RPCMessage>&);
  std::shared_ptr<HTTPConnection> route_request(ConnectionType, int32_t, int);
  void dump_entire_state();
  int get_sockfd() { return sockfd; }

private:
  // PPM-related functions
  void send_dn(struct sockaddr_in, const std::string &, size_t, RPCID,
               Priority);
  std::unique_ptr<Buffer>
  prepare_credit_return(const std::unique_ptr<Buffer> &);
  std::tuple<const std::string &, bool, size_t, RPCID>
  credit_post_process(const std::unique_ptr<Buffer> &);
  bool check_credit_available(std::string_view);
  void check_credit_transmission();
  float get_theo_term(std::string_view, bool);
  void update_limits(int32_t, std::string_view);
  void fanout_req_management(RPCID, const std::string &,
                             const std::unique_ptr<Buffer> &);
  void send_sub_request(const std::shared_ptr<RPCMessage>&);
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
  Stats &stats;
};