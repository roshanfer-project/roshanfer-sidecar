# pragma once

#include "buffer_manager.h"
#include "connection.h"
#include "http2_parser.h"
#include "stats.h"
#include <cstdint>
#include <memory>
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


class gRPCParser {
    public:
        gRPCParser(std::vector<std::unique_ptr<RPCMessage>>&, BufferManager&,
                Stats&, std::vector<std::unique_ptr<RPCMessage>>&);
        void parse(HTTPConnection& conn, Buffer& buffer);
        void clear(int);
    
    private:
        std::vector<std::unique_ptr<RPCMessage>>& ppm_queue;
        std::vector<std::unique_ptr<RPCMessage>>& ingress_req_queue;
        BufferManager& buffer_manager;
        Stats& stats;
        
        // fd -> (stream_id -> RPCMessage)
        std::unordered_map<int, std::unordered_map<uint32_t, RPCMessage>> partial_messages;
};