#include "rpc_message.h"
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>
#include <glog/logging.h>

HeaderField::HeaderField(const uint8_t* name, size_t name_len, const uint8_t* value, size_t value_len) 
: name(), name_len(name_len), value(), value_len(value_len) {
    if (name_len > sizeof(this->name) || value_len > sizeof(this->value)) {
        LOG(FATAL) << "HeaderField name or value too long";
    }
    std::memcpy(this->name, name, name_len);
    std::memcpy(this->value, value, value_len);
}

RPCMessage::RPCMessage(uint32_t ds_stream_id, int fd)
:   data_map(std::unordered_map<uint8_t, DataReadStruct>()),
    req_headers(std::vector<std::unique_ptr<HeaderField>>()),
    res_headers(std::vector<std::unique_ptr<HeaderField>>()),
    res_trailers(std::vector<std::unique_ptr<HeaderField>>()),
    ds_stream_id(ds_stream_id),
    us_stream_id(-1),
    ds_fd(fd),
    us_fd(-1),
    error(false)
{
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
    }

    auto hf = std::make_unique<HeaderField>(
        HeaderField(
            name,
            name_len,
            value,
            value_len
        )
    );

    if (tailer) {
        if (request) {
            LOG(FATAL) << "Tailer in request";
        }
        res_trailers.push_back(std::move(hf));
    } else {
        if (request) {
            req_headers.push_back(std::move(hf));
        } else {
            res_headers.push_back(std::move(hf));
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
}