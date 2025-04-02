#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sys/types.h>
#include <vector>
#include <chrono>

class HeaderField {
    public:
        HeaderField(const uint8_t* name, size_t name_len, const uint8_t* value, size_t value_len);

    public:
        uint8_t name[60];
        size_t name_len;
        uint8_t value[60];
        size_t value_len;
};

typedef struct DataReadStruct {
    const uint8_t* data;
    size_t len;
} DataReadStruct;

class RPCMessage {

    public:
        RPCMessage(uint32_t, int);
        ~RPCMessage();
        void add_header_field(const uint8_t*, size_t, const uint8_t*, size_t, bool, bool);
        void add_data(const uint8_t*, size_t, bool);
    
    public:
        uint32_t ds_stream_id;
        uint32_t us_stream_id;
        DataReadStruct req_data;
        DataReadStruct res_data;
        bool have_req_data;
        bool have_res_data;
        int ds_fd;
        std::vector<std::unique_ptr<HeaderField>> req_headers;
        std::vector<std::unique_ptr<HeaderField>> res_headers;
        std::vector<std::unique_ptr<HeaderField>> res_trailers;
        std::chrono::time_point<std::chrono::system_clock> req_rcv_time;
        std::chrono::time_point<std::chrono::system_clock> req_for_time;
        std::chrono::time_point<std::chrono::system_clock> res_rcv_time;
};