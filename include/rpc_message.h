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
    size_t read_offset;
} DataReadStruct;

class RPCMessage {

    public:
        RPCMessage();

        // getter and setters
        uint32_t get_ds_stream_id() const { return ds_stream_id; }
        void set_ds_stream_id(uint32_t id) { ds_stream_id = id; }
        int get_ds_fd() const { return ds_fd; }
        void set_ds_fd(int fd) { ds_fd = fd; }
        uint32_t get_us_stream_id() const { return us_stream_id; }
        void set_us_stream_id(uint32_t id) { us_stream_id = id; }
        int get_us_fd() const { return us_fd; }
        void set_us_fd(int fd) { us_fd = fd; }

        
        virtual void add_header_field(const uint8_t*, size_t, const uint8_t*, size_t, bool, bool) = 0;
        virtual void add_data(const uint8_t*, size_t, bool) = 0;
        virtual void clear() = 0;
        virtual std::string& get_service() = 0;
        virtual std::string& get_method() = 0;
        virtual bool is_error() = 0;
        virtual void set_error(bool) = 0;
        virtual bool is_http() = 0;
    
    protected:
        // downstream identifiers
        uint32_t ds_stream_id;
        int ds_fd;

        // upstream identifiers
        uint32_t us_stream_id;
        int us_fd;

    public:
        std::chrono::time_point<std::chrono::system_clock> req_rcv_time;
        std::chrono::time_point<std::chrono::system_clock> req_for_time;
        std::chrono::time_point<std::chrono::system_clock> res_rcv_time;
};

class gRPCMessage : public RPCMessage {
    public:
        gRPCMessage();
        ~gRPCMessage();
        
        // virtual methods
        void add_header_field(const uint8_t*, size_t, const uint8_t*, size_t, bool, bool);
        void add_data(const uint8_t*, size_t, bool);
        void clear();
        bool is_error() { return error; };
        void set_error(bool err) { error = err; }
        std::string& get_service() { return service; }
        std::string& get_method() { return method; }
        bool is_http() { return false; }

        
        std::unordered_map<uint8_t, DataReadStruct>& get_data_map() { return data_map; }
        std::vector<HeaderField*>& get_req_headers() { return req_headers; }
        int get_req_header_count() const { return req_header_count; }
        std::vector<HeaderField*>& get_res_headers() { return res_headers; }
        int get_res_header_count() const { return res_header_count; }
        std::vector<HeaderField*>& get_res_trailers() { return res_trailers; }
        int get_res_trailer_count() const { return res_trailer_count; }
        void set_method(const std::string& m) { method = m; }


    private:
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
};

class HTTPMessage : public RPCMessage {
    public:
        HTTPMessage();
        ~HTTPMessage();

        // virtual methods
        void add_header_field(const uint8_t*, size_t, const uint8_t*, size_t, bool, bool);
        void add_data(const uint8_t*, size_t, bool);
        void clear();
        bool is_error() { return error; };
        void set_error(bool err) { error = err; }
        std::string& get_service() { return service; }
        std::string& get_method() { return method; }
        bool is_http() { return true; }

        void set_method(const char* m, size_t m_len) {
            method.assign(m, m_len);
        }
        void set_service(const char* s, size_t s_len);
        void set_path(const char* p, size_t p_len) {
            path.assign(p, p_len);
        }
        const std::string& get_path() const { return path; }
        DataReadStruct& get_res_data() { return res_data; }
        std::vector<HeaderField*>& get_req_headers() { return req_headers; }
        int get_req_header_count() const { return req_header_count; }
        std::vector<HeaderField*>& get_res_headers() { return res_headers; }
        int get_res_header_count() const { return res_header_count; }
        void set_minor(int m) { minor = m; }
        int get_minor() const { return minor; }
        void set_status(int s) { status = s; }
        int get_status() const { return status; }
        void set_msg(const char* m, size_t m_len) {
            msg.assign(m, m_len);
        }
        const std::string& get_msg() const { return msg; }

    private:
        std::string service;
        std::string path;
        std::string method;
        int minor;
        int status;
        std::string msg;


        bool error;

        std::vector<HeaderField*> req_headers;
        int req_header_count;
        std::vector<HeaderField*> res_headers;
        int res_header_count;

        DataReadStruct res_data;
};


class RPCMessagePool {
    public:
        RPCMessagePool(int, int);
        void free_rpc(RPCMessage*);
        RPCMessage* get_rpc(uint32_t, int, bool);
    
    private:
        std::queue<RPCMessage*> grpc_pool;
        std::queue<RPCMessage*> http_pool;
};