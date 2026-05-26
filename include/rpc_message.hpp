#pragma once

#include "buffer.hpp"
#include "connection_enums.hpp"
#include "fast_map.hpp"
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <queue>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <vector>

const size_t MAX_HEADER_FIELD_SIZE = 60;

extern int64_t local_id_counter;

class HeaderField {
public:
  HeaderField();
  void set(const uint8_t *name, size_t name_len, const uint8_t *value,
           size_t value_len);

public:
  std::array<uint8_t, MAX_HEADER_FIELD_SIZE> name;
  size_t name_len{0};
  std::array<uint8_t, MAX_HEADER_FIELD_SIZE> value;
  size_t value_len{0};
};

const size_t MAX_PAYLOAD_SIZE = 20000;
const size_t MAX_HEADER_FIELD_NUMBER = 13;

class DataReadStruct {
public:
  DataReadStruct();

public:
  std::array<uint8_t, MAX_PAYLOAD_SIZE> data;
  size_t offset{0};
  size_t read_offset{0};
};

using RPCID = int64_t;
using Priority = int32_t;

class RPCMessage {

public:
  RPCMessage();
  virtual ~RPCMessage();

  // delete copy constructor and assignment operator
  RPCMessage(const RPCMessage &) = delete;
  RPCMessage &operator=(const RPCMessage &) = delete;

  // delete move constructor and assignment operator
  RPCMessage(RPCMessage &&) = delete;
  RPCMessage &operator=(RPCMessage &&) = delete;

  // getter and setters
  [[nodiscard]] int32_t get_ds_stream_id() const { return ds_stream_id; }
  void set_ds_stream_id(int32_t ds_id) { ds_stream_id = ds_id; }
  [[nodiscard]] int get_ds_fd() const { return ds_fd; }
  void set_ds_fd(int fd) { ds_fd = fd; }
  [[nodiscard]] int32_t get_us_stream_id() const { return us_stream_id; }
  void set_us_stream_id(int32_t us_id) { us_stream_id = us_id; }
  [[nodiscard]] int get_us_fd() const { return us_fd; }
  void set_us_fd(int fd) { us_fd = fd; }
  [[nodiscard]] RPCID get_local_id() const { return local_id; }
  std::string get_id_string() {
    return std::format("(global: {}, local: {})", global_id, local_id);
  }
  void set_local_id(RPCID new_id) { local_id = new_id; }
  [[nodiscard]] ConnectionType get_type() const { return type; }
  void set_type(ConnectionType new_type) { type = new_type; }
  [[nodiscard]] Priority get_priority() const { return priority; }
  void set_priority(Priority new_priority) { priority = new_priority; }

  virtual void add_header_field(const uint8_t *, size_t, const uint8_t *,
                                size_t, bool, bool) = 0;
  virtual void set_local_id_header() = 0;
  virtual void add_data(const uint8_t *, size_t, bool) = 0;
  virtual void clear() = 0;
  virtual std::string &get_service() = 0;
  virtual std::string &get_method() = 0;
  virtual bool is_error() = 0;
  virtual void set_error(bool) = 0;
  virtual HTTP http() = 0;
  virtual bool is_drop() = 0;
  virtual void dump_req_headers() = 0;

protected:
  // downstream identifiers
  int32_t ds_stream_id{0};
  int ds_fd{-1};

  // upstream identifiers
  int32_t us_stream_id{0};
  int us_fd{-1};

  RPCID local_id{-1};
  RPCID global_id{-1};
  Priority priority;
  ConnectionType type;

public:
  std::chrono::time_point<std::chrono::steady_clock> req_rcv_time;
  std::chrono::time_point<std::chrono::steady_clock> req_for_time;
  std::chrono::time_point<std::chrono::steady_clock> res_rcv_time;

  // parallel fan-out counters
  uint8_t pfanout_req;
  uint8_t pfanout_res;

  // dynamic fan-out credit return queue
  std::queue<std::unique_ptr<Buffer>> credit_return_queue;
  std::string *dfanout_service;

  std::chrono::time_point<std::chrono::steady_clock> deadline;

  const uint8_t *RPC_LOCAL_ID_HEADER_NAME =
      reinterpret_cast<const uint8_t *>("rpc-local-id");
  const size_t RPC_LOCAL_ID_HEADER_NAME_LEN = 12;
  std::array<char, 32> rpc_local_id_header_value;
};

class gRPCMessage : public RPCMessage {
public:
  gRPCMessage();
  ~gRPCMessage() override;

  // delete copy semantics
  gRPCMessage(const gRPCMessage &) = delete;
  gRPCMessage &operator=(const gRPCMessage &) = delete;

  // delete move semantics
  gRPCMessage(gRPCMessage &&) = delete;
  gRPCMessage &operator=(gRPCMessage &&) = delete;

  // virtual methods
  void add_header_field(const uint8_t *, size_t, const uint8_t *, size_t, bool,
                        bool) override;
  void set_local_id_header() override;
  void add_data(const uint8_t *, size_t, bool) override;
  void clear() override;
  bool is_error() override { return error; };
  void set_error(bool err) override { error = err; }
  std::string &get_service() override { return service; }
  std::string &get_method() override { return method; }
  HTTP http() override { return HTTP::HTTP2; }
  bool is_drop() override { return false; } // gRPC messages are never dropped
  void dump_req_headers() override {
    for (auto &h : req_headers) {
      LOG(INFO) << "Header: " << std::string(h->name.begin(), h->name.end())
                << " " << std::string(h->value.begin(), h->value.end());
    }
  }

  std::unordered_map<uint8_t, DataReadStruct *> &get_data_map() {
    return data_map;
  }
  std::vector<HeaderField *> &get_req_headers() { return req_headers; }
  [[nodiscard]] size_t get_req_header_count() const { return req_header_count; }
  std::vector<HeaderField *> &get_res_headers() { return res_headers; }
  [[nodiscard]] size_t get_res_header_count() const { return res_header_count; }
  std::vector<HeaderField *> &get_res_trailers() { return res_trailers; }
  [[nodiscard]] size_t get_res_trailer_count() const { return res_trailer_count; }
  void set_method(const std::string &m) { method = m; }

private:
  // routing and ppm related
  std::string service;
  std::string method;

  bool error{false};
  std::unordered_map<uint8_t, DataReadStruct *> data_map; // 0: req, 1: res
  std::vector<HeaderField *> req_headers;
  size_t req_header_count{0};
  std::vector<HeaderField *> res_headers;
  size_t res_header_count{0};
  std::vector<HeaderField *> res_trailers;
  size_t res_trailer_count{0};
};

class HTTPMessage : public RPCMessage {
public:
  HTTPMessage();
  ~HTTPMessage() override;

  // delete copy semantics
  HTTPMessage(const HTTPMessage &) = delete;
  HTTPMessage &operator=(const HTTPMessage &) = delete;

  // delete move semantics
  HTTPMessage(HTTPMessage &&) = delete;
  HTTPMessage &operator=(HTTPMessage &&) = delete;

  // virtual methods
  void add_header_field(const uint8_t *, size_t, const uint8_t *, size_t, bool,
                        bool) override;
  void set_local_id_header() override;
  void add_data(const uint8_t *, size_t, bool) override;
  void clear() override;
  bool is_error() override { return error; };
  void set_error(bool err) override { error = err; }
  std::string &get_service() override { return service; }
  std::string &get_method() override { return method; }
  HTTP http() override { return HTTP::HTTP1; }
  bool is_drop() override { return error && status == 503; }
  void dump_req_headers() override {
    for (auto &h : req_headers) {
      LOG(INFO) << "Header: " << std::string(h->name.begin(), h->name.end())
                << " " << std::string(h->value.begin(), h->value.end());
    }
  }

  void set_method(const char *m, size_t m_len) { method.assign(m, m_len); }
  void set_service(const char *s, size_t s_len);
  /** First path segment (same semantics as set_service). False = invalid URL
   * form. */
  static bool parse_service_from_request_target(const char *s, size_t s_len,
                                                std::string *out);
  void set_path(const char *p, size_t p_len) { path.assign(p, p_len); }
  [[nodiscard]] const std::string &get_path() const { return path; }
  DataReadStruct &get_res_data() { return *res_data; }
  std::vector<HeaderField *> &get_req_headers() { return req_headers; }
  [[nodiscard]] size_t get_req_header_count() const { return req_header_count; }
  std::vector<HeaderField *> &get_res_headers() { return res_headers; }
  [[nodiscard]] size_t get_res_header_count() const { return res_header_count; }
  void set_minor(int m) { minor = m; }
  [[nodiscard]] int get_minor() const { return minor; }
  void set_status(int s) { status = s; }
  [[nodiscard]] int get_status() const { return status; }
  void set_msg(const char *m, size_t m_len) { msg.assign(m, m_len); }
  [[nodiscard]] const std::string &get_msg() const { return msg; }

private:
  std::string service;
  std::string path;
  std::string method;
  int minor;
  int status;
  std::string msg;

  bool error{false};

  std::vector<HeaderField *> req_headers;
  size_t req_header_count{0};
  std::vector<HeaderField *> res_headers;
  size_t res_header_count{0};

  DataReadStruct *res_data;
};

class RPCMessagePool {
public:
  RPCMessagePool(int, int);
  void free_rpc(std::shared_ptr<RPCMessage>);
  std::shared_ptr<RPCMessage> get_rpc(int32_t, int, HTTP, ConnectionType);

private:
  std::queue<std::shared_ptr<RPCMessage>> grpc_pool;
  std::queue<std::shared_ptr<RPCMessage>> http_pool;
};