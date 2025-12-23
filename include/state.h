#pragma once

#include "buffer.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "connection_enums.h"
#include "fast_map.hpp"
#include "hdr/hdr_histogram.h"
#include "ingress.h"
#include "ppm_queue.h"
#include "ring_wrapper.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "spinlock.hpp"
#include "stats.h"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
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

class FailedDNInfo {
public:
  FailedDNInfo();

  // delete copy semantics
  FailedDNInfo(const FailedDNInfo &) = delete;
  FailedDNInfo &operator=(const FailedDNInfo &) = delete;

  // delete move semantics
  FailedDNInfo(FailedDNInfo &&) = delete;
  FailedDNInfo &operator=(FailedDNInfo &&) = delete;

  void push_back(std::unique_ptr<Buffer>);
  void push_front(std::unique_ptr<Buffer>);
  std::unique_ptr<Buffer> pop();
  // std::string id_list();
  size_t size();

public:
  std::deque<std::unique_ptr<Buffer>> failed_dn_info;

private:
  SpinLock lock;
  std::atomic<size_t> _size;
};

class SharedState {
public:
  SharedState(std::vector<std::string>, std::vector<std::string>);

public:
  /*ConnectionType::INGRESS-side metrics*/

  std::atomic<int32_t> in_flight;
  std::atomic<int32_t> in_local;

  FailedDNInfo failed_dn_info;

  /*ConnectionType::EGRESS-side metrics*/
};

class LocalState {
public:
  LocalState(std::vector<std::string>, std::vector<std::string>);

public:
  /*ConnectionType::INGRESS-side metrics*/

  /*ConnectionType::EGRESS-side metrics*/

  // number of drops (updated if only config.is_ingress is true)
  uint32_t drops;
  /*
  READ-ONLY: ONLY used by Ingress::Ingress to track the ingress limit.
  */
  LocalMap<int32_t> ingress_limit;
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
  void queue_multiplexer(const std::unique_ptr<Buffer> &);
  void ppm_client(bool, const std::unique_ptr<Buffer> &);
  void ingress_admit();
  struct hdr_histogram *get_histogram() { return hist; }

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
  void send_dn(struct sockaddr_in, const std::string &, size_t, int32_t);
  std::tuple<const std::string &, bool, size_t, int32_t>
  valid_credit(const char *);
  bool check_credit_available();
  void check_credit_transmission();

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
  struct hdr_histogram *hist;
  std::chrono::steady_clock::time_point next_hist_update;
  int thread_id;

public:
  SharedState &shared_state;
  LocalState local_state;
  Utilization utilization;
  std::string &ingress_service;
  Stats stats;
};