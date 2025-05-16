#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <queue>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <chrono>

class HeaderField {
    public:
        HeaderField();
        void set(const uint8_t* name, size_t name_len, const uint8_t* value, size_t value_len);

    public:
        uint8_t name[60];
        size_t name_len;
        uint8_t value[60];
        size_t value_len;
};

const size_t MAX_PAYLOAD_SIZE = 20000;
const size_t MAX_HEADER_FIELD_NUMBER = 10;

typedef struct DataReadStruct {
    const uint8_t* data;
    size_t offset;
} DataReadStruct;

class RPCMessage {

    public:
        RPCMessage();
        ~RPCMessage();
        void add_header_field(const uint8_t*, size_t, const uint8_t*, size_t, bool, bool);
        void add_data(const uint8_t*, size_t, bool);
        std::string& get_service() { return service; }
    
    public:
        // downstream identifiers
        uint32_t ds_stream_id;
        int ds_fd;

        // upstream identifiers
        uint32_t us_stream_id;
        int us_fd;
        

        // routing and ppm related
        std::string service;
        std::string method;

        bool error;
        std::unordered_map<uint8_t, DataReadStruct> data_map; // 0: req, 1: res
        std::vector<HeaderField*> req_headers;
        int req_header_count;
        std::vector<HeaderField*> res_headers;
        int res_header_count;
        std::vector<HeaderField*> res_trailers;
        int res_trailer_count;
        std::chrono::time_point<std::chrono::system_clock> req_rcv_time;
        std::chrono::time_point<std::chrono::system_clock> req_for_time;
        std::chrono::time_point<std::chrono::system_clock> res_rcv_time;
};


class RPCMessagePool {
    public:
        RPCMessagePool(int);
        void free_rpc(RPCMessage*);
        RPCMessage* get_rpc(uint32_t, int);
    
    private:
        std::queue<RPCMessage*> pool;
};