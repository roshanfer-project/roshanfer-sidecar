#include "rpc_message.h"
#include <cstdint>
#include <cstring>
#include <vector>
#include <glog/logging.h>

HeaderField::HeaderField()
: name(), name_len(0), value(), value_len(0) {}

void HeaderField::set(const uint8_t* name, size_t name_len, const uint8_t* value, size_t value_len) {
    if (name_len > sizeof(this->name) || value_len > sizeof(this->value)) {
        LOG(FATAL) << "HeaderField name or value too long";
    }
    std::memcpy(this->name, name, name_len);
    this->name[name_len] = '\0';
    this->name_len = name_len;
    std::memcpy(this->value, value, value_len);
    this->value[value_len] = '\0';
    this->value_len = value_len;
}

RPCMessage::RPCMessage()
:   data_map(std::unordered_map<uint8_t, DataReadStruct>()),
    req_headers(std::vector<HeaderField*>()),
    res_headers(std::vector<HeaderField*>()),
    res_trailers(std::vector<HeaderField*>()),
    ds_stream_id(-1),
    us_stream_id(-1),
    ds_fd(-1),
    us_fd(-1),
    error(false),
    req_header_count(0),
    res_header_count(0),
    res_trailer_count(0)
{
    for (int i = 0; i < MAX_HEADER_FIELD_NUMBER; i++) {
        req_headers.push_back(new HeaderField());
        res_headers.push_back(new HeaderField());
        res_trailers.push_back(new HeaderField());
    }
    data_map.emplace(0, DataReadStruct{nullptr, 0});
    data_map.emplace(1, DataReadStruct{nullptr, 0});
}


void RPCMessage::add_header_field(const uint8_t* name, size_t name_len,
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
        res_trailers[res_trailer_count]->set(name, name_len, value, value_len);
        res_trailer_count++;
        //res_trailers.push_back(hf);
    } else {
        if (request) {
            if (req_header_count >= MAX_HEADER_FIELD_NUMBER) {
                LOG(FATAL) << "Too many headers";
            }
            req_headers[req_header_count]->set(name, name_len, value, value_len);
            req_header_count++;
            //req_headers.push_back(hf);
        } else {
            if (res_header_count >= MAX_HEADER_FIELD_NUMBER) {
                LOG(FATAL) << "Too many headers";
            }
            res_headers[res_header_count]->set(name, name_len, value, value_len);
            res_header_count++;
            //res_headers.push_back(hf);
        }
    }
}

void RPCMessage::add_data(const uint8_t* data, size_t len, bool request) {
    uint8_t key = request ? 0 : 1;
    auto& data_struct = this->data_map[key];

    if (data_struct.data == nullptr) {
        data_struct.data = new uint8_t[MAX_PAYLOAD_SIZE];
        data_struct.offset = 0;
    }

    if (data_struct.offset + len > MAX_PAYLOAD_SIZE) {
        LOG(FATAL) << "Data length exceeds maximum payload size";
    }
    std::memcpy(const_cast<uint8_t*>(data_struct.data) + data_struct.offset, data, len);
    data_struct.offset += len;
    VLOG(1) << "Add data (request:" << request << ") of length: " << len << " for stream id: " << ds_stream_id 
                << " us_stream_id: " << us_stream_id;
}

RPCMessage::~RPCMessage() {
    VLOG(1) << "RPCMessage deconstructor on ds_id: " << ds_stream_id 
                << " ds_fd: " << ds_fd
                << " us_id: " << us_stream_id
                << " us_fd: " << us_fd;
    delete [] data_map[0].data;
    delete [] data_map[1].data;
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


RPCMessagePool::RPCMessagePool(int n)
:   pool(std::queue<RPCMessage*>()) {
    for (int i = 0; i < n; i++) {
        auto rpc = new RPCMessage();
        free_rpc(rpc);
    }
}

void RPCMessagePool::free_rpc(RPCMessage* rpc) {
    pool.push(rpc);
    rpc->ds_stream_id = -1;
    rpc->ds_fd = -1;
    rpc->us_stream_id = -1;
    rpc->us_fd = -1;
    rpc->error = false;
    rpc->service.clear();
    rpc->method.clear();
    rpc->data_map[0].offset = 0;
    rpc->data_map[0].read_offset = 0;
    rpc->data_map[1].offset = 0;
    rpc->data_map[1].read_offset = 0;
    for (auto& header : rpc->req_headers) {
        header->name_len = 0;
        header->value_len = 0;
    }
    rpc->req_header_count = 0;
    for (auto& header : rpc->res_headers) {
        header->name_len = 0;
        header->value_len = 0;
    }
    rpc->res_header_count = 0;
    for (auto& header : rpc->res_trailers) {
        header->name_len = 0;
        header->value_len = 0;
    }
    rpc->res_trailer_count = 0;
    rpc->req_for_time = std::chrono::time_point<std::chrono::system_clock>();
    rpc->req_rcv_time = std::chrono::time_point<std::chrono::system_clock>();
    rpc->res_rcv_time = std::chrono::time_point<std::chrono::system_clock>();
}

RPCMessage* RPCMessagePool::get_rpc(uint32_t ds_stream_id, int ds_fd) {
    if (pool.empty()) {
        LOG(FATAL) << "No RPCMessage available in the pool";
    }
    auto rpc = pool.front();
    pool.pop();
    rpc->ds_stream_id = ds_stream_id;
    rpc->ds_fd = ds_fd;
    return rpc;
}
