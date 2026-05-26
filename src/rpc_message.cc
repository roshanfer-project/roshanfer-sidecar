#include "rpc_message.hpp"
#include "config.hpp"
#include "connection_enums.hpp"
#include "fast_map.hpp"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <glog/logging.h>
#include <queue>
#include <string>
#include <sys/types.h>
#include <utility>
#include <vector>

RPCID local_id_counter = 1;

static inline RPCID get_new_local_id(RPCID parent) {
  local_id_counter++;
  const auto branch = static_cast<uint32_t>(local_id_counter);
  const uint64_t packed =
      (static_cast<uint64_t>(static_cast<uint32_t>(parent)) << 32) |
      static_cast<uint64_t>(branch);
  return static_cast<RPCID>(packed);
}

//////// RPCMessage Implementation

RPCMessage::RPCMessage() : credit_return_queue() {}

RPCMessage::~RPCMessage() {
  LOG(FATAL) << "RPC Message deconstructor (should not be called)";
}

//////// gRPCMessage Implementation

HeaderField::HeaderField()
    : name(std::array<uint8_t, MAX_HEADER_FIELD_SIZE>()),
      value(std::array<uint8_t, MAX_HEADER_FIELD_SIZE>()) {}

void HeaderField::set(const uint8_t *field_name, size_t field_name_len,
                      const uint8_t *field_value, size_t field_value_len) {
  if (field_name_len > name.size()) {
    LOG(FATAL) << "HeaderField name is too long"
               << ", len: " << field_name_len
               << ", name: " << std::string(field_name, field_name)
               << ", value: " << std::string(field_value, field_value)
               << ", value_len: " << field_value_len;
  }
  if (field_value_len > value.size()) {
    LOG(FATAL) << "HeaderField value is too long"
               << ", len: " << field_value_len
               << ", value: " << std::string(field_value, field_value)
               << ", name: " << std::string(field_name, field_name)
               << ", name_len: " << field_name_len;
  }
  std::copy_n(field_name, field_name_len, this->name.begin());
  this->name.at(field_name_len) = '\0';
  this->name_len = field_name_len;
  std::copy_n(field_value, field_value_len, this->value.begin());
  this->value.at(field_value_len) = '\0';
  this->value_len = field_value_len;
}

DataReadStruct::DataReadStruct()
    : data(std::array<uint8_t, MAX_PAYLOAD_SIZE>()) {}

gRPCMessage::gRPCMessage()
    : RPCMessage(), data_map(std::unordered_map<uint8_t, DataReadStruct *>()),
      req_headers(std::vector<HeaderField *>()),
      res_headers(std::vector<HeaderField *>()),
      res_trailers(std::vector<HeaderField *>()) {
  for (int i = 0; std::cmp_less(i, MAX_HEADER_FIELD_NUMBER); i++) {
    req_headers.push_back(new HeaderField());
    res_headers.push_back(new HeaderField());
    res_trailers.push_back(new HeaderField());
  }
  data_map.emplace(0, new DataReadStruct());
  data_map.emplace(1, new DataReadStruct());
}

void gRPCMessage::add_header_field(const uint8_t *name, size_t name_len,
                                   const uint8_t *value, size_t value_len,
                                   bool request, bool tailer) {

  if (std::strcmp(reinterpret_cast<const char *>(name), ":path") == 0) {
    service.assign(reinterpret_cast<const char *>(value + 1), value_len - 1);
    method.clear();
    VLOG(1) << "Service: " << service << " Method: " << method;
    // VLOG(1) << "Service: " << service;
  }

  if (request) {
    if (std::memcmp(name, "rpc-id", 6) == 0) {
      global_id = static_cast<RPCID>(std::stoll(
          std::string(reinterpret_cast<const char *>(value), value_len)));
      if (config.is_ingress) {
        local_id = get_new_local_id(0);
      } else {
        // only set local id if we are on the INGRESS-side. The EGRESS-side
        // should be set by rpc-local-id branch
        local_id =
            type == ConnectionType::INGRESS ? get_new_local_id(0) : local_id;
      }
      VLOG(1) << "RPC ID after reading global id: " << get_id_string();
    }
    if (std::memcmp(name, "rpc-local-id", 12) == 0) {
      auto id_from_str = static_cast<RPCID>(std::stoll(
          std::string(reinterpret_cast<const char *>(value), value_len)));
      if (type == ConnectionType::INGRESS) {
        if (id_from_str == -1) {
          LOG(FATAL) << "rpc-local-id is -1 on INGRESS side";
        }
        VLOG(1) << "RPC ID after reading local id: " << get_id_string();
      } else if (type == ConnectionType::EGRESS) {
        if (id_from_str <= 0 || id_from_str > static_cast<RPCID> INT32_MAX) {
          LOG(FATAL)
              << "local id: " << id_from_str
              << " received from rpc-local-id header is not in valid range";
        }
        local_id = get_new_local_id(id_from_str);
        VLOG(1) << "RPC ID after reading local id: " << get_id_string();
        return; // we don't want to actually send rpc-local-id to downstream
                // services
      }
    }
    if (std::memcmp(name, "priority", 8) == 0) {
      priority = static_cast<Priority>(std::stoll(
          std::string(reinterpret_cast<const char *>(value), value_len)));
      VLOG(1) << "Priority: " << priority;
    }
  }

  if (tailer) {
    if (request) {
      LOG(FATAL) << "Tailer in request";
    }
    if (res_trailer_count >= MAX_HEADER_FIELD_NUMBER) {
      LOG(FATAL) << "Too many trailers";
    }
    res_trailers.at(res_trailer_count)->set(name, name_len, value, value_len);
    res_trailer_count++;
  } else {
    if (request) {
      if (req_header_count >= MAX_HEADER_FIELD_NUMBER) {
        LOG(FATAL) << "Too many headers";
      }
      req_headers.at(req_header_count)->set(name, name_len, value, value_len);
      req_header_count++;
    } else {
      if (res_header_count >= MAX_HEADER_FIELD_NUMBER) {
        LOG(FATAL) << "Too many headers";
      }
      res_headers.at(res_header_count)->set(name, name_len, value, value_len);
      res_header_count++;
    }
  }
}

void gRPCMessage::set_local_id_header() {
  rpc_local_id_header_value.fill(0);
  auto len = static_cast<size_t>(std::snprintf(
      rpc_local_id_header_value.data(), rpc_local_id_header_value.size(),
      "%lld", static_cast<long long>(local_id)));
  add_header_field(
      RPC_LOCAL_ID_HEADER_NAME, RPC_LOCAL_ID_HEADER_NAME_LEN,
      reinterpret_cast<const uint8_t *>(rpc_local_id_header_value.data()), len,
      true, false);
}

void gRPCMessage::add_data(const uint8_t *data, size_t len, bool request) {
  uint8_t key = request ? 0 : 1;
  auto &data_struct = this->data_map.at(key);

  if (len > MAX_PAYLOAD_SIZE - data_struct->offset) {
    LOG(FATAL) << "Data length exceeds maximum payload size"
               << " , len: " << len << " , offset: " << data_struct->offset
               << " , max: " << MAX_PAYLOAD_SIZE;
  }
  std::copy_n(data, len, data_struct->data.begin() + data_struct->offset);
  data_struct->offset += len;
  if (VLOG_IS_ON(3)) {
    VLOG(3) << "Data added to gRPCMessage: "
            << std::string(
                   reinterpret_cast<const char *>(data_struct->data.data()),
                   data_struct->offset);
  }
  VLOG(1) << "Add data (request:" << request << ") of length: " << len
          << " for stream id: " << ds_stream_id
          << " us_stream_id: " << us_stream_id;
}

void gRPCMessage::clear() {
  VLOG(3) << "Clearing gRPCMessage for ds_id: " << ds_stream_id
          << " ds_fd: " << ds_fd << " us_id: " << us_stream_id
          << " us_fd: " << us_fd;
  ds_stream_id = -1;
  ds_fd = -1;
  us_stream_id = -1;
  us_fd = -1;
  error = false;
  service.clear();
  method.clear();
  data_map.at(0)->offset = 0;
  data_map.at(0)->read_offset = 0;
  data_map.at(1)->offset = 0;
  data_map.at(1)->read_offset = 0;
  for (auto &header : req_headers) {
    header->name_len = 0;
    header->value_len = 0;
  }
  req_header_count = 0;
  for (auto &header : res_headers) {
    header->name_len = 0;
    header->value_len = 0;
  }
  res_header_count = 0;
  for (auto &header : res_trailers) {
    header->name_len = 0;
    header->value_len = 0;
  }
  res_trailer_count = 0;
  req_for_time = std::chrono::time_point<std::chrono::steady_clock>();
  req_rcv_time = std::chrono::time_point<std::chrono::steady_clock>();
  res_rcv_time = std::chrono::time_point<std::chrono::steady_clock>();
  local_id = -1;
  global_id = -1;
  pfanout_req = 0;
  pfanout_res = 0;
  if (credit_return_queue.size() != 0) {
    LOG(FATAL) << "credit_return queue is not empty";
  }
  dfanout_service = nullptr;
}

gRPCMessage::~gRPCMessage() {
  VLOG(1) << "RPCMessage deconstructor on ds_id: " << ds_stream_id
          << " ds_fd: " << ds_fd << " us_id: " << us_stream_id
          << " us_fd: " << us_fd;
  try {
    delete data_map.at(0);
    delete data_map.at(1);
  } catch (std::exception &e) {
    LOG(FATAL) << "execption raised: " << e.what();
  }
  for (auto &header : req_headers) {
    delete header;
  }
  for (auto &header : res_headers) {
    delete header;
  }
  for (auto &header : res_trailers) {
    delete header;
  }
}

//////// HTTPMessage Implementation

HTTPMessage::HTTPMessage()
    : RPCMessage(), req_headers(std::vector<HeaderField *>()),
      res_headers(std::vector<HeaderField *>()),
      res_data(new DataReadStruct()) {
  for (int i = 0; std::cmp_less(i, MAX_HEADER_FIELD_NUMBER); i++) {
    req_headers.push_back(new HeaderField());
    res_headers.push_back(new HeaderField());
  }
}

bool HTTPMessage::parse_service_from_request_target(const char *s, size_t s_len,
                                                    std::string *out) {
  const char *const end = s + s_len;
  const char *p = s;
  if (s_len >= 7) {
    if (std::memcmp(s, "http://", 7) == 0) {
      p += 7;
    }
  }

  const char *slash = static_cast<const char *>(
      std::memchr(p, '/', static_cast<size_t>(end - p)));
  if (!slash || slash + 1 >= end) {
    return false;
  }

  const char *svc_begin = slash + 1;
  const char *question = static_cast<const char *>(
      std::memchr(svc_begin, '?', static_cast<size_t>(end - svc_begin)));
  const char *svc_end = question ? question : end;

  out->assign(svc_begin, static_cast<size_t>(svc_end - svc_begin));
  return true;
}

void HTTPMessage::set_service(const char *s, size_t s_len) {
  /*
      Extracts the main endpoint path as the service.
      Exmaple:

      if the input s is:
      http://192.168.1.100:2000/hotels?lat=37.7867&lon=-122.4112&inDate=2024-08-15&outDate=2024-08-17:GET

      The service is "hotels"
  */

  if (!parse_service_from_request_target(s, s_len, &service)) {
    service.clear();
    LOG(FATAL) << "Invalid service format"
               << ", string: " << std::string(s, s_len) << ", len: " << s_len;
  }
}

void HTTPMessage::add_header_field(const uint8_t *name, size_t name_len,
                                   const uint8_t *value, size_t value_len,
                                   bool request, bool tailer) {

  if (tailer) {
    LOG(FATAL) << "Tailers are not supported in HTTPMessage";
  }

  if (request) {
    if (std::memcmp(name, "rpc-id", 6) == 0) {
      global_id = static_cast<RPCID>(std::stoll(
          std::string(reinterpret_cast<const char *>(value), value_len)));
      if (config.is_ingress) {
        local_id = get_new_local_id(0);
      } else {
        // only set local id if we are on the INGRESS-side. The EGRESS-side
        // should be set by rpc-local-id branch
        local_id =
            type == ConnectionType::INGRESS ? get_new_local_id(0) : local_id;
      }
      VLOG(1) << "RPC ID after reading global id: " << get_id_string();
    }
    if (std::memcmp(name, "rpc-local-id", 12) == 0) {
      auto id_from_str = static_cast<RPCID>(std::stoll(
          std::string(reinterpret_cast<const char *>(value), value_len)));
      if (type == ConnectionType::INGRESS) {
        if (id_from_str == -1) {
          LOG(FATAL) << "rpc-local-id is -1 on INGRESS side";
        }
        VLOG(1) << "RPC ID after reading local id: " << get_id_string();
      } else if (type == ConnectionType::EGRESS) {
        if (id_from_str <= 0 || id_from_str > static_cast<RPCID> INT32_MAX) {
          LOG(FATAL)
              << "local id: " << id_from_str
              << " received from rpc-local-id header is not in valid range";
        }
        local_id = get_new_local_id(id_from_str);
        VLOG(1) << "RPC ID after reading local id: " << get_id_string();
        return; // we don't want to actually send rpc-local-id to downstream
                // services
      }
    }
    if (std::memcmp(name, "priority", 8) == 0) {
      priority = static_cast<Priority>(std::stoll(
          std::string(reinterpret_cast<const char *>(value), value_len)));
      VLOG(1) << "Priority: " << priority;
    }
  }

  if (request) {
    if (req_header_count >= MAX_HEADER_FIELD_NUMBER) {
      LOG(FATAL) << "Too many headers in HTTP request";
    }
    req_headers.at(req_header_count)->set(name, name_len, value, value_len);
    req_header_count++;
  } else {
    if (res_header_count >= MAX_HEADER_FIELD_NUMBER) {
      LOG(FATAL) << "Too many headers in HTTP response";
    }
    res_headers.at(res_header_count)->set(name, name_len, value, value_len);
    res_header_count++;
  }
}

void HTTPMessage::set_local_id_header() {
  rpc_local_id_header_value.fill(0);
  auto len = static_cast<size_t>(std::snprintf(
      rpc_local_id_header_value.data(), rpc_local_id_header_value.size(),
      "%lld", static_cast<long long>(local_id)));
  add_header_field(
      RPC_LOCAL_ID_HEADER_NAME, RPC_LOCAL_ID_HEADER_NAME_LEN,
      reinterpret_cast<const uint8_t *>(rpc_local_id_header_value.data()), len,
      true, false);
}

void HTTPMessage::add_data(const uint8_t *data, size_t len, bool request) {
  if (request) {
    LOG(FATAL) << "Request data is not supported in HTTPMessage";
  }

  if (len > MAX_PAYLOAD_SIZE - res_data->offset) {
    LOG(FATAL) << "Data length exceeds maximum payload size"
               << " , len: " << len << " , offset: " << res_data->offset
               << " , max: " << MAX_PAYLOAD_SIZE;
  }
  std::copy_n(data, len, res_data->data.begin() + res_data->offset);
  res_data->offset += len;
  VLOG(1) << "(HTTPMessage) Add response data of length: " << len
          << " for ds_id: " << ds_stream_id << " us_id: " << us_stream_id;
}

void HTTPMessage::clear() {
  VLOG(3) << "Clearing HTTPMessage for ds_id: " << ds_stream_id
          << " ds_fd: " << ds_fd << " us_id: " << us_stream_id
          << " us_fd: " << us_fd;
  ds_stream_id = -1;
  ds_fd = -1;
  us_stream_id = -1;
  us_fd = -1;
  error = false;
  service.clear();
  method.clear();
  res_data->offset = 0;
  res_data->read_offset = 0;
  for (auto &header : req_headers) {
    header->name_len = 0;
    header->value_len = 0;
  }
  req_header_count = 0;
  for (auto &header : res_headers) {
    header->name_len = 0;
    header->value_len = 0;
  }
  res_header_count = 0;
  msg.clear();
  minor = 0;
  status = 0;
  req_for_time = std::chrono::time_point<std::chrono::steady_clock>();
  req_rcv_time = std::chrono::time_point<std::chrono::steady_clock>();
  res_rcv_time = std::chrono::time_point<std::chrono::steady_clock>();
  local_id = -1;
  global_id = -1;
  pfanout_req = 0;
  pfanout_res = 0;
  if (credit_return_queue.size() != 0) {
    LOG(FATAL) << "credit_return queue is not empty";
  }
  dfanout_service = nullptr;
}

HTTPMessage::~HTTPMessage() {
  VLOG(1) << "HTTPMessage deconstructor on ds_id: " << ds_stream_id
          << " ds_fd: " << ds_fd << " us_id: " << us_stream_id
          << " us_fd: " << us_fd;
  delete res_data;
  for (auto &header : req_headers) {
    delete header;
  }
  for (auto &header : res_headers) {
    delete header;
  }
}

RPCMessagePool::RPCMessagePool(int grpc_n, int http_n)
    : grpc_pool(std::queue<std::shared_ptr<RPCMessage>>()),
      http_pool(std::queue<std::shared_ptr<RPCMessage>>()) {

  for (int i = 0; i < http_n; i++) {
    auto rpc = std::make_shared<HTTPMessage>();
    free_rpc(std::move(rpc));
  }

  for (int i = 0; i < grpc_n; i++) {
    auto rpc = std::make_shared<gRPCMessage>();
    free_rpc(std::move(rpc));
  }
}

void RPCMessagePool::free_rpc(std::shared_ptr<RPCMessage> rpc) {
  if (!rpc) {
    LOG(FATAL) << "RPCMessage is null";
  }
  if (rpc.use_count() > 1) {
    LOG(FATAL) << "RPCMessage is still in use, count: " << rpc.use_count();
  }
  rpc->clear();
  if (rpc->http() == HTTP::HTTP1) {
    http_pool.push(std::move(rpc));
  } else {
    grpc_pool.push(std::move(rpc));
  }
}

std::shared_ptr<RPCMessage> RPCMessagePool::get_rpc(int32_t ds_stream_id,
                                                    int ds_fd, HTTP http,
                                                    ConnectionType type) {
  std::queue<std::shared_ptr<RPCMessage>> &pool =
      http == HTTP::HTTP1 ? http_pool : grpc_pool;
  if (pool.empty()) {
    LOG(FATAL) << "No RPCMessage available in the pool";
  }
  auto rpc = std::move(pool.front());
  pool.pop();
  rpc->set_ds_fd(ds_fd);
  rpc->set_ds_stream_id(ds_stream_id);
  rpc->set_type(type);
  return rpc;
}
