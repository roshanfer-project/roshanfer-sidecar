#pragma once

#include "buffer.h"
#include "connection_enums.h"
#include "ingress.h"
#include "netdb.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <arpa/inet.h>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <netinet/in.h>
#include <nghttp2/nghttp2.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class NoConnectionException : public std::runtime_error {
public:
  NoConnectionException(std::string msg) : std::runtime_error(msg) {}
};

class HTTPParseException : public std::runtime_error {
public:
  HTTPParseException(std::string msg) : std::runtime_error(msg) {}
};

class BufferFullException : public std::runtime_error {
public:
  BufferFullException(const uint8_t *outbuf_ptr, ssize_t written)
      : std::runtime_error(""), outbuf_ptr(outbuf_ptr), written(written) {}

public:
  const uint8_t *outbuf_ptr;
  ssize_t written;
};

using FD = int;

typedef struct CallbackData {
  ConnectionType type;
  ConnectionDirection direction;
  int fd;
  RPCQueue *queue;
  RPCMapper *mapper;
  ConnectionStatus *status;
  Stats *stats;
} CallbackData;

class HTTPConnection {

public:
  /**
   * @brief Construct an upstream connection
   * @note This is used by state
   */
  HTTPConnection(std::string, uint16_t, ConnectionType, Stats *);
  HTTPConnection(std::string, uint16_t, struct sockaddr_in, ConnectionType,
                 Stats *);

  /*
   * @brief Construct an downstream connection
   * @note This is used by listeners
   */
  HTTPConnection(int, ConnectionType, Stats *);
  virtual ~HTTPConnection();

  // delete copy semantics
  HTTPConnection(const HTTPConnection &) = delete;
  HTTPConnection &operator=(const HTTPConnection &) = delete;

  // delete move semantics
  HTTPConnection(HTTPConnection &&) = delete;
  HTTPConnection &operator=(HTTPConnection &&) = delete;

  int get_fd() { return fd; }
  struct sockaddr_in get_addr_in();
  struct sockaddr *get_addr() {
    return reinterpret_cast<struct sockaddr *>(&addr);
  }
  std::string type_to_str();
  std::string direction_to_str();
  ConnectionStatus get_status() { return status; }
  void set_status(ConnectionStatus s) { status = s; }
  uint16_t get_port() { return port; }
  std::string &get_host() { return host; }

  // pure virtual functions
  virtual void http_read(const std::unique_ptr<Buffer> &, Ingress &) = 0;
  virtual bool want_write() = 0;
  virtual int http_write(const std::unique_ptr<Buffer> &) = 0;
  virtual void submit_settings() = 0;
  virtual int32_t submit_request(std::shared_ptr<RPCMessage>) = 0;
  virtual void submit_response(std::shared_ptr<RPCMessage>) = 0;
  virtual void submit_error_response(std::shared_ptr<RPCMessage>) = 0;
  virtual bool available() = 0;
  virtual HTTP http() = 0;

protected:
  int fd; // local socket file descriptor
  struct sockaddr_in addr;
  ConnectionStatus status;
  std::string host;
  uint16_t port;
  Stats *stats;

public:
  ConnectionType type;
  ConnectionDirection direction;
};

class HTTP2Connection : public HTTPConnection {

public:
  HTTP2Connection(std::string, uint16_t, ConnectionType, RPCQueue *,
                  RPCMapper *, Stats *);
  HTTP2Connection(std::string, uint16_t, struct sockaddr_in, ConnectionType,
                  RPCQueue *, RPCMapper *, Stats *);
  HTTP2Connection(int, ConnectionType, RPCMapper *, RPCQueue *, Stats *);
  ~HTTP2Connection();

  // delete copy semantics
  HTTP2Connection(const HTTP2Connection &) = delete;
  HTTP2Connection &operator=(const HTTP2Connection &) = delete;

  // delete move semantics
  HTTP2Connection(HTTP2Connection &&) = delete;
  HTTP2Connection &operator=(HTTP2Connection &&) = delete;

  void http_read(const std::unique_ptr<Buffer> &, Ingress &);
  bool want_write();
  int http_write(const std::unique_ptr<Buffer> &);
  void submit_settings();
  int32_t submit_request(std::shared_ptr<RPCMessage>);
  void submit_response(std::shared_ptr<RPCMessage>);
  void submit_error_response(std::shared_ptr<RPCMessage>);
  bool available();
  HTTP http() { return HTTP::HTTP2; }

private:
  nghttp2_session *session;
  nghttp2_session_callbacks *callbacks;
  std::unique_ptr<CallbackData> callback_data;

private:
  static void set_callbacks(nghttp2_session_callbacks *);
};

const size_t HTTP1Connection_BUF_SIZE = 200000;
const size_t HTTP1Connection_MAX_HEADERS = 10;

class HTTP1Connection : public HTTPConnection {

public:
  HTTP1Connection(std::string, uint16_t, ConnectionType, RPCMapper *,
                  RPCQueue *, Stats *);
  HTTP1Connection(std::string, uint16_t, struct sockaddr_in, ConnectionType,
                  RPCMapper *, RPCQueue *, Stats *);
  HTTP1Connection(int, ConnectionType, RPCMapper *, RPCQueue *, Stats *);
  ~HTTP1Connection();

  // delete copy semantics
  HTTP1Connection(const HTTP1Connection &) = delete;
  HTTP1Connection &operator=(const HTTP1Connection &) = delete;

  // delete move semantics
  HTTP1Connection(HTTP1Connection &&) = delete;
  HTTP1Connection &operator=(HTTP1Connection &&) = delete;

  void http_read(const std::unique_ptr<Buffer> &, Ingress &);
  bool want_write();
  int http_write(const std::unique_ptr<Buffer> &);
  void submit_settings();
  int32_t submit_request(std::shared_ptr<RPCMessage>);
  void submit_response(std::shared_ptr<RPCMessage>);
  void submit_error_response(std::shared_ptr<RPCMessage>);
  bool available();
  HTTP http() { return HTTP::HTTP1; }

private:
  void set_rpc_message(std::shared_ptr<HTTPMessage> msg);
  std::shared_ptr<HTTPMessage> get_rpc_message();
  int parse_http1_request(Buffer *);

private:
  // internal state for parsing
  std::unique_ptr<std::array<char, HTTP1Connection_BUF_SIZE>> buf;
  size_t buf_len;
  size_t prev_buf_len;
  bool hdr_complete;
  int content_length;
  int hdr_size;

  bool idle;
  RPCMapper *mapper;
  RPCQueue *queue;
  int32_t last_id;
  std::shared_ptr<HTTPMessage> rpc_message;
};

typedef struct LBBind {
  FD lb_fd;
  ReplicaIndex replica_index;
} LBBind;

class ReplicaPool {
public:
  ReplicaPool(ReplicaIndex, struct sockaddr_in, ConnectionType);

  // delete copy semantics
  ReplicaPool(const ReplicaPool &) = delete;
  ReplicaPool &operator=(const ReplicaPool &) = delete;

  // delete move semantics
  ReplicaPool(ReplicaPool &&) = delete;
  ReplicaPool &operator=(ReplicaPool &&) = delete;

  std::shared_ptr<HTTPConnection>
  add_connection(const std::string &, int, RPCMapper *, RPCQueue *, HTTP,
                 Stats *, struct sockaddr_in * = nullptr);
  std::shared_ptr<HTTPConnection>
  get_any_conn(const std::unordered_set<FD> * = nullptr);
  std::shared_ptr<HTTPConnection> get_connection(int fd);
  struct sockaddr_in get_addr() { return addr; }
  ReplicaIndex get_index() { return index; }

private:
  std::unordered_map<int, std::shared_ptr<HTTPConnection>>
      connections; // fd: connection
  std::unordered_map<int, std::shared_ptr<HTTPConnection>>::iterator
      next_conn{};
  struct sockaddr_in addr;
  ConnectionType type;
  ReplicaIndex index;
};

class ConnectionPool {

public:
  ConnectionPool(ConnectionType);

  std::shared_ptr<ReplicaPool> lb();
  struct sockaddr_in acquire(RPCID);
  std::shared_ptr<HTTPConnection> peek(RPCID);
  void release(RPCID);

  std::shared_ptr<ReplicaPool> &add_replica(struct sockaddr_in addr_in) {
    auto [it, ok] = replicas.emplace(
        max_replica_index,
        std::make_shared<ReplicaPool>(max_replica_index, addr_in, type));
    if (!ok) {
      LOG(FATAL) << "Could not insert new ReplicaPool";
    }
    waitings.init(max_replica_index);
    max_replica_index++;
    return it->second;
  }
  std::shared_ptr<ReplicaPool> get_replica_pool(ReplicaIndex index) {
    auto it = replicas.find(index);
    if (it == replicas.end()) {
      LOG(FATAL) << "replica with index " << index << " not found";
    }
    return it->second;
  }
  size_t get_num_replicas() { return replicas.size(); }

private:
  std::unordered_map<ReplicaIndex, std::shared_ptr<ReplicaPool>> replicas;
  ReplicaIndex max_replica_index = 0;
  ConnectionType type;
  KeyValueMinTracker waitings{};
  std::unordered_map<RPCID, LBBind> bindings{};
  std::unordered_set<FD> binded_fds{};
};

inline bool same_sockaddr_in(const struct sockaddr_in &a,
                             const struct sockaddr_in &b) {
  return a.sin_addr.s_addr == b.sin_addr.s_addr && a.sin_port == b.sin_port;
}

inline std::vector<struct sockaddr_in> name_resolver(std::string host,
                                                     int port) {
  LOG(INFO) << "resolving Ip addresses for host: " << host;
  struct addrinfo hints, *result;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET; // IPv4
  hints.ai_socktype = SOCK_STREAM;

  int rv =
      getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result);
  if (rv != 0) {
    LOG(FATAL) << "DNS resolution failed for " << host << ": "
               << gai_strerror(rv);
  }

  std::vector<struct sockaddr_in> output_list;

  auto orig = result;
  while (result != nullptr) {
    output_list.push_back(
        *reinterpret_cast<struct sockaddr_in *>(result->ai_addr));
    result = result->ai_next;
  }

  freeaddrinfo(orig);

  std::vector<struct sockaddr_in> deduped;
  for (const auto &addr : output_list) {
    bool seen = false;
    for (const auto &existing : deduped) {
      if (same_sockaddr_in(existing, addr)) {
        seen = true;
        break;
      }
    }
    if (!seen) {
      deduped.push_back(addr);
    }
  }
  output_list = std::move(deduped);

  LOG(INFO) << "List (length " << output_list.size() << ") of resolved IPs for "
            << host;
  for (auto addr : output_list) {
    std::array<char, INET_ADDRSTRLEN> ip_str{};
    if (inet_ntop(AF_INET, &addr.sin_addr, ip_str.data(), ip_str.size()) ==
        nullptr) {
      LOG(FATAL) << "Failed to covert resolved address to binary";
    } else {
      LOG(INFO) << ip_str.data();
    }
  }

  return output_list;
}