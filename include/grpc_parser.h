# pragma once

#include "buffer_manager.h"
#include "connection.h"
#include "http2_parser.h"
#include "stats.h"
#include <cstdint>
#include <unordered_map>
#include <vector>

class LocalgRPCParser {
    public:
        LocalgRPCParser(bool);
        bool add_frame(HTTP2Frame& frame);
        std::vector<HTTP2Frame*> get(uint32_t);
        std::vector<HTTP2Frame*> get_all();
        void remove(uint32_t stream_id) { map.erase(stream_id); }

    private:
        std::unordered_map<uint32_t, std::vector<HTTP2Frame*>> map;
        bool request;
};

class RPCMessage {

    public:
        RPCMessage(bool, Buffer*);
        bool add_frame(HTTP2Frame, char[]);
        std::vector<HTTP2Frame>& get_frames() { return frames; }
        uint32_t get_length() { return length; }
        Buffer* get_buffer() { return buffer; }

    private:
        std::vector<HTTP2Frame> frames;
        bool request;
        uint32_t length;
        Buffer* buffer;
};


class gRPCParser {
    public:
        gRPCParser(std::vector<Buffer*>&, BufferManager& buffer_manager,
                Stats& stats);
        void parse(HTTPConnection& conn, Buffer& buffer);
        void clear(int);
    
    private:
        std::vector<Buffer*>& ppm_queue;
        BufferManager& buffer_manager;
        Stats& stats;
        
        // fd -> (stream_id -> RPCMessage)
        std::unordered_map<int, std::unordered_map<uint32_t, RPCMessage>> partial_messages;
};