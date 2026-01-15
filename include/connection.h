#pragma once

#include "buffer.h"
#include "connection_enums.h"
#include "ingress.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
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

class ConnectionPool {

public:
  ConnectionPool(ConnectionType);

  /**
   * @brief Add a connection to the pool
   */
  std::shared_ptr<HTTPConnection> add_connection(const std::string &, int,
                                                 RPCMapper *, RPCQueue *, HTTP,
                                                 Stats *);
  std::shared_ptr<HTTPConnection> get_connection(int fd);
  std::shared_ptr<HTTPConnection> get_any_connection();
  bool has_connection(int fd);
  void remove_connection(int fd);
  struct sockaddr_in get_addr();

private:
  std::unordered_map<int, std::shared_ptr<HTTPConnection>>
      connections; // fd: connection
  ConnectionType type;
  struct sockaddr_in addr;
  bool addr_set;
  std::unordered_map<int, std::shared_ptr<HTTPConnection>>::iterator next_conn;
};