#include "rpc_message.h"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <queue>
#include <vector>
#include <glog/logging.h>


//////// RPCMessage Implementation

RPCMessage::RPCMessage()
: ds_stream_id(0), ds_fd(-1), us_stream_id(0), us_fd(-1) {}


//////// gRPCMessage Implementation

HeaderField::HeaderField()
: name(std::array<uint8_t, MAX_HEADER_FIELD_SIZE>()), name_len(0),
  value(std::array<uint8_t, MAX_HEADER_FIELD_SIZE>()), value_len(0) {
}

void HeaderField::set(const uint8_t* field_name, size_t field_name_len, const uint8_t* field_value, size_t field_value_len) {
    if (field_name_len >= name.size() || field_value_len >= value.size()){
        LOG(FATAL) << "HeaderField name or value too long";
    }
    std::copy_n(field_name, field_name_len, this->name.begin());
    this->name.at(field_name_len) = '\0';
    this->name_len = field_name_len;
    std::copy_n(field_value, field_value_len, this->value.begin());
    this->value.at(field_value_len) = '\0';
    this->value_len = field_value_len;
}

DataReadStruct::DataReadStruct()
: data(std::array<uint8_t, MAX_PAYLOAD_SIZE>()), offset(0), read_offset(0) {}

gRPCMessage::gRPCMessage()
:   RPCMessage(),
    error(false),
    data_map(std::unordered_map<uint8_t, DataReadStruct*>()),
    req_headers(std::vector<HeaderField*>()),
    req_header_count(0),
    res_headers(std::vector<HeaderField*>()),
    res_header_count(0),
    res_trailers(std::vector<HeaderField*>()),
    res_trailer_count(0)
{
    for (int i = 0; i < (int)MAX_HEADER_FIELD_NUMBER; i++) {
        req_headers.push_back(new HeaderField());
        res_headers.push_back(new HeaderField());
        res_trailers.push_back(new HeaderField());
    }
    data_map.emplace(0, new DataReadStruct());
    data_map.emplace(1, new DataReadStruct());
}


void gRPCMessage::add_header_field(const uint8_t* name, size_t name_len,
    const uint8_t* value, size_t value_len, bool request, bool tailer) {
    
    if (std::strcmp(reinterpret_cast<const char*>(name), ":path") == 0) {
        // split the path by /
        std::string path(reinterpret_cast<const char*>(value), value_len);
        size_t pos = path.find("/", 1);
        if (pos != std::string::npos) {
            service = path.substr(1, pos-1);
            method = path.substr(pos+1);
        } else {
            LOG(FATAL) << "Invalid path: " << path;
        }
        VLOG(1) << "Service: " << service << " Method: " << method;
        //VLOG(1) << "Service: " << service;
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

void gRPCMessage::add_data(const uint8_t* data, size_t len, bool request) {
    uint8_t key = request ? 0 : 1;
    auto& data_struct = this->data_map.at(key);


    if (data_struct->offset + len > MAX_PAYLOAD_SIZE) {
        LOG(FATAL) << "Data length exceeds maximum payload size";
    }
    std::copy_n(data, len, data_struct->data.begin() + data_struct->offset);
    data_struct->offset += len;
    VLOG(1) << "Add data (request:" << request << ") of length: " << len << " for stream id: " << ds_stream_id 
                << " us_stream_id: " << us_stream_id;
}


void gRPCMessage::clear() {
    VLOG(1) << "Clearing gRPCMessage for ds_id: " << ds_stream_id 
                << " ds_fd: " << ds_fd
                << " us_id: " << us_stream_id
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
    for (auto& header : req_headers) {
        header->name_len = 0;
        header->value_len = 0;
    }
    req_header_count = 0;
    for (auto& header : res_headers) {
        header->name_len = 0;
        header->value_len = 0;
    }
    res_header_count = 0;
    for (auto& header : res_trailers) {
        header->name_len = 0;
        header->value_len = 0;
    }
    res_trailer_count = 0;
    req_for_time = std::chrono::time_point<std::chrono::steady_clock>();
    req_rcv_time = std::chrono::time_point<std::chrono::steady_clock>();
    res_rcv_time = std::chrono::time_point<std::chrono::steady_clock>();
}

gRPCMessage::~gRPCMessage() {
    VLOG(1) << "RPCMessage deconstructor on ds_id: " << ds_stream_id 
                << " ds_fd: " << ds_fd
                << " us_id: " << us_stream_id
                << " us_fd: " << us_fd;
    delete data_map.at(0);
    delete data_map.at(1);
    for (auto& header : req_headers) {
        delete header;
    }
    for (auto& header : res_headers) {
        delete header;
    }
    for (auto& header : res_trailers) {
        delete header;
    }
}


//////// HTTPMessage Implementation

HTTPMessage::HTTPMessage()
:   RPCMessage(),
    error(false),
    req_headers(std::vector<HeaderField*>()),
    req_header_count(0),
    res_headers(std::vector<HeaderField*>()),
    res_header_count(0),
    res_data(new DataReadStruct())
{
    for (int i = 0; i < (int)MAX_HEADER_FIELD_NUMBER; i++) {
        req_headers.push_back(new HeaderField());
        res_headers.push_back(new HeaderField());
    }
}


void HTTPMessage::set_service(const char* s, size_t s_len) {
    /*
        Extracts the main endpoint path as the service.
        Exmaple:

        if the input s is:
        http://192.168.1.100:2000/hotels?lat=37.7867&lon=-122.4112&inDate=2024-08-15&outDate=2024-08-17:GET

        The service is "hotels"
    */

    const char* const end = s + s_len;

    // (1) skip "http://"
    const char* p = s;
    if (s_len >= 7) {
        if (std::memcmp(s, "http://", 7) == 0) {
            p += 7;
        }
    }

    // (2) find the first '/' after host:port
    const char* slash = static_cast<const char*>(std::memchr(p, '/', (size_t)(end - p)));
    if (!slash || slash + 1 >= end) {
        service.clear();
        LOG(FATAL) << "Invalid service format";
    }

    // (3) service starts just after that '/'
    const char* svc_begin = slash + 1;

    // (4) find the '?' that marks end-of-service
    const char* question = static_cast<const char*>(std::memchr(svc_begin, '?', (size_t)(end - svc_begin)));
    const char* svc_end = question ? question : end;

    service.assign(svc_begin, (size_t)(svc_end - svc_begin));
}

void HTTPMessage::add_header_field(const uint8_t* name, size_t name_len,
    const uint8_t* value, size_t value_len, bool request, bool tailer) {
    
    if (tailer) {
        LOG(FATAL) << "Tailers are not supported in HTTPMessage";
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

void HTTPMessage::add_data(const uint8_t* data, size_t len, bool request) {
    if (request) {
        LOG(FATAL) << "Request data is not supported in HTTPMessage";
    }

    if (res_data->offset + len > MAX_PAYLOAD_SIZE || len > MAX_PAYLOAD_SIZE) {
        LOG(FATAL) << "Data length exceeds maximum payload size"
                    << " , len: " << len
                    << " , offset: " << res_data->offset
                    << " , max: " << MAX_PAYLOAD_SIZE;
    }
    std::copy_n(data, len, res_data->data.begin() + res_data->offset);
    res_data->offset += len;
    VLOG(1) << "(HTTPMessage) Add response data of length: " << len << " for ds_id: " << ds_stream_id 
                << " us_id: " << us_stream_id;
}

void HTTPMessage::clear() {
    VLOG(1) << "Clearing HTTPMessage for ds_id: " << ds_stream_id 
                << " ds_fd: " << ds_fd
                << " us_id: " << us_stream_id
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
    for (auto& header : req_headers) {
        header->name_len = 0;
        header->value_len = 0;
    }
    req_header_count = 0;
    for (auto& header : res_headers) {
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
}

HTTPMessage::~HTTPMessage() {
    VLOG(1) << "HTTPMessage deconstructor on ds_id: " << ds_stream_id 
                << " ds_fd: " << ds_fd
                << " us_id: " << us_stream_id
                << " us_fd: " << us_fd;
    delete res_data;
    for (auto& header : req_headers) {
        delete header;
    }
    for (auto& header : res_headers) {
        delete header;
    }
}

RPCMessagePool::RPCMessagePool(int grpc_n, int http_n)
:   grpc_pool(std::queue<RPCMessage*>()),
    http_pool(std::queue<RPCMessage*>()) {

    for (int i = 0; i < http_n; i++) {
        auto rpc = new HTTPMessage();
        free_rpc(rpc);
    }

    for (int i = 0; i < grpc_n; i++) {
        auto rpc = new gRPCMessage();
        free_rpc(rpc);
    }
}

void RPCMessagePool::free_rpc(RPCMessage* rpc) {
    rpc->clear();
    if (rpc->http() == HTTP::HTTP1) {
        http_pool.push(rpc);
    } else {
        grpc_pool.push(rpc);
    }
}

RPCMessage* RPCMessagePool::get_rpc(int32_t ds_stream_id, int ds_fd, HTTP http) {
    std::queue<RPCMessage*>& pool = http == HTTP::HTTP1 ? http_pool : grpc_pool;
    if (pool.empty()) {
        LOG(FATAL) << "No RPCMessage available in the pool";
    }
    auto rpc = pool.front();
    pool.pop();
    rpc->set_ds_fd(ds_fd);
    rpc->set_ds_stream_id(ds_stream_id);
    return rpc;
}
