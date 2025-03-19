#pragma once

#include "http2_parser.h"
#include "buffer.h"
#include <vector>
#include <chrono>

class RPCMessage {

    public:
        RPCMessage(bool, Buffer*);
        bool add_frame(HTTP2Frame, char[]);
        std::vector<HTTP2Frame>& get_frames() { return frames; }
        uint32_t get_length() { return length; }
        Buffer* get_buffer() { return buffer; }
        void set_rcv_time();
        std::chrono::time_point<std::chrono::system_clock> get_rcv_time() { return rcv_time; }

    private:
        std::vector<HTTP2Frame> frames;
        bool request;
        uint32_t length;
        Buffer* buffer;
        std::chrono::time_point<std::chrono::system_clock> rcv_time;
};