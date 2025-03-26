#include "rpc_message.h"
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>
#include <glog/logging.h>

RPCMessage::RPCMessage(uint32_t ds_stream_id, int fd)
:   req_data(DataReadStruct{nullptr, 0}),
    res_data(DataReadStruct{nullptr, 0}),
    req_headers(std::vector<std::unique_ptr<HeaderField>>()),
    res_headers(std::vector<std::unique_ptr<HeaderField>>()),
    res_trailers(std::vector<std::unique_ptr<HeaderField>>()),
    ds_stream_id(ds_stream_id),
    us_stream_id(-1),
    have_req_data(false),
    have_res_data(false),
    ds_fd(fd)
{}


void RPCMessage::add_header_field(const uint8_t* name, size_t name_len,
    const uint8_t* value, size_t value_len, bool request, bool tailer) {
    auto hf = std::make_unique<HeaderField>(
        HeaderField {
            std::string(reinterpret_cast<const char*>(name), name_len),
            name_len,
            std::string(reinterpret_cast<const char*>(value), value_len),
            value_len
        }
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
    if (request) {
        if (have_req_data) {
            LOG(FATAL) << "Request data already exists (Multiple DATA frames)";
        }
        req_data.data = new uint8_t[len];
        std::memcpy(const_cast<uint8_t*>(req_data.data), data, len);
        req_data.len = len;
        have_req_data = true;
    } else {
        if (have_res_data) {
            LOG(FATAL) << "Response data already exists (Multiple DATA frames)";
        }
        res_data.data = new uint8_t[len];
        std::memcpy(const_cast<uint8_t*>(res_data.data), data, len);
        res_data.len = len;
        have_res_data = true;
    }
}

void RPCMessage::set_rcv_time() {
    rcv_time = std::chrono::system_clock::now();
}

RPCMessage::~RPCMessage() {
    DLOG(INFO) << "RPCMessage deconstructor on ds_stream_id: " << ds_stream_id 
                << " us_stream_id: " << us_stream_id;
    delete [] req_data.data;
    delete [] res_data.data;
}