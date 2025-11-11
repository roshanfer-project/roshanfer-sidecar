#pragma once

#include "connection_enums.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <queue>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <chrono>

const size_t MAX_HEADER_FIELD_SIZE = 60;

class HeaderField {
    public:
        HeaderField();
        void set(const uint8_t* name, size_t name_len, const uint8_t* value, size_t value_len);

    public:
        std::array<uint8_t, MAX_HEADER_FIELD_SIZE> name;
        size_t name_len;
        std::array<uint8_t, MAX_HEADER_FIELD_SIZE> value;
        size_t value_len;
};

const size_t MAX_PAYLOAD_SIZE = 20000;
const size_t MAX_HEADER_FIELD_NUMBER = 12;

class DataReadStruct {
    public:
        DataReadStruct();

    public:
        std::array<uint8_t, MAX_PAYLOAD_SIZE> data;
        size_t offset;
        size_t read_offset;
};

class RPCMessage {

    public:
        RPCMessage();
        virtual ~RPCMessage();

        // delete copy constructor and assignment operator
        RPCMessage(const RPCMessage&) = delete;
        RPCMessage& operator=(const RPCMessage&) = delete;

        // delete move constructor and assignment operator
        RPCMessage(RPCMessage&&) = delete;
        RPCMessage& operator=(RPCMessage&&) = delete;

        // getter and setters
        int32_t get_ds_stream_id() const { return ds_stream_id; }
        void set_ds_stream_id(int32_t ds_id) { ds_stream_id = ds_id; }
        int get_ds_fd() const { return ds_fd; }
        void set_ds_fd(int fd) { ds_fd = fd; }
        int32_t get_us_stream_id() const { return us_stream_id; }
        void set_us_stream_id(int32_t us_id) { us_stream_id = us_id; }
        int get_us_fd() const { return us_fd; }
        void set_us_fd(int fd) { us_fd = fd; }
        int16_t get_id() const { return id; }
        void set_id(int16_t new_id) { id = new_id; }
        
        virtual void add_header_field(const uint8_t*, size_t, const uint8_t*, size_t, bool, bool) = 0;
        virtual void add_data(const uint8_t*, size_t, bool) = 0;
        virtual void clear() = 0;
        virtual std::string& get_service() = 0;
        virtual std::string& get_method() = 0;
        virtual bool is_error() = 0;
        virtual void set_error(bool) = 0;
        virtual HTTP http() = 0;
        virtual bool is_drop() = 0;
    
    protected:
        // downstream identifiers
        int32_t ds_stream_id;
        int ds_fd;

        // upstream identifiers
        int32_t us_stream_id;
        int us_fd;

        int16_t id;

    public:
        std::chrono::time_point<std::chrono::steady_clock> req_rcv_time;
        std::chrono::time_point<std::chrono::steady_clock> req_for_time;
        std::chrono::time_point<std::chrono::steady_clock> res_rcv_time;
};

class gRPCMessage : public RPCMessage {
    public:
        gRPCMessage();
        ~gRPCMessage();

        // delete copy semantics
        gRPCMessage(const gRPCMessage&) = delete;
        gRPCMessage& operator=(const gRPCMessage&) = delete;

        // delete move semantics
        gRPCMessage(gRPCMessage&&) = delete;
        gRPCMessage& operator=(gRPCMessage&&) = delete;
        
        // virtual methods
        void add_header_field(const uint8_t*, size_t, const uint8_t*, size_t, bool, bool);
        void add_data(const uint8_t*, size_t, bool);
        void clear();
        bool is_error() { return error; };
        void set_error(bool err) { error = err; }
        std::string& get_service() { return service; }
        std::string& get_method() { return method; }
        HTTP http() { return HTTP::HTTP2; }
        bool is_drop() { return false; } // gRPC messages are never dropped

        
        std::unordered_map<uint8_t, DataReadStruct*>& get_data_map() { return data_map; }
        std::vector<HeaderField*>& get_req_headers() { return req_headers; }
        size_t get_req_header_count() const { return req_header_count; }
        std::vector<HeaderField*>& get_res_headers() { return res_headers; }
        size_t get_res_header_count() const { return res_header_count; }
        std::vector<HeaderField*>& get_res_trailers() { return res_trailers; }
        size_t get_res_trailer_count() const { return res_trailer_count; }
        void set_method(const std::string& m) { method = m; }


    private:
        // routing and ppm related
        std::string service;
        std::string method;

        bool error;
        std::unordered_map<uint8_t, DataReadStruct*> data_map; // 0: req, 1: res
        std::vector<HeaderField*> req_headers;
        size_t req_header_count;
        std::vector<HeaderField*> res_headers;
        size_t res_header_count;
        std::vector<HeaderField*> res_trailers;
        size_t res_trailer_count;
};

class HTTPMessage : public RPCMessage {
    public:
        HTTPMessage();
        ~HTTPMessage();

        // delete copy semantics
        HTTPMessage(const HTTPMessage&) = delete;
        HTTPMessage& operator=(const HTTPMessage&) = delete;

        // delete move semantics
        HTTPMessage(HTTPMessage&&) = delete;
        HTTPMessage& operator=(HTTPMessage&&) = delete;

        // virtual methods
        void add_header_field(const uint8_t*, size_t, const uint8_t*, size_t, bool, bool);
        void add_data(const uint8_t*, size_t, bool);
        void clear();
        bool is_error() { return error; };
        void set_error(bool err) { error = err; }
        std::string& get_service() { return service; }
        std::string& get_method() { return method; }
        HTTP http() { return HTTP::HTTP1; }
        bool is_drop() { return error && status == 503; }

        void set_method(const char* m, size_t m_len) {
            method.assign(m, m_len);
        }
        void set_service(const char* s, size_t s_len);
        void set_path(const char* p, size_t p_len) {
            path.assign(p, p_len);
        }
        const std::string& get_path() const { return path; }
        DataReadStruct& get_res_data() { return *res_data; }
        std::vector<HeaderField*>& get_req_headers() { return req_headers; }
        size_t get_req_header_count() const { return req_header_count; }
        std::vector<HeaderField*>& get_res_headers() { return res_headers; }
        size_t get_res_header_count() const { return res_header_count; }
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
        size_t req_header_count;
        std::vector<HeaderField*> res_headers;
        size_t res_header_count;

        DataReadStruct* res_data;
};


class RPCMessagePool {
    public:
        RPCMessagePool(int, int);
        void free_rpc(std::shared_ptr<RPCMessage>);
        std::shared_ptr<RPCMessage> get_rpc(int32_t, int, HTTP);
    
    private:
        std::queue<std::shared_ptr<RPCMessage>> grpc_pool;
        std::queue<std::shared_ptr<RPCMessage>> http_pool;
};