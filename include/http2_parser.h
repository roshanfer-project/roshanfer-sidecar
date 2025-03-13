#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

const char HTTP2_PREFACE[25] = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"; 

enum class FRAMETYPE {
    HEADERS, DATA, SETTINGS, PREFACE, WINDOW_UPDATE, PING, OTHERS,
    GOAWAY
};

typedef struct HTTP2Frame {
    uint32_t stream_id;
    FRAMETYPE type;
    bool EOS;
    bool EOH;
    uint32_t offset;
    uint32_t length;

    public:
        std::string to_string() {
            return "stream_id: " + std::to_string(stream_id) + ", type: " + type_to_string() + ", EOS: " + std::to_string(EOS) + ", EOH: " + std::to_string(EOH) + ", offset: " + std::to_string(offset) + ", length: " + std::to_string(length);
        }
    private:
        std::string type_to_string() {
            switch (type) {
                case FRAMETYPE::HEADERS:
                    return "HEADERS";
                case FRAMETYPE::DATA:
                    return "DATA";
                case FRAMETYPE::SETTINGS:
                    return "SETTINGS";
                case FRAMETYPE::PREFACE:
                    return "PREFACE";
                case FRAMETYPE::WINDOW_UPDATE:
                    return "WINDOW_UPDATE";
                case FRAMETYPE::PING:
                    return "PING";
                case FRAMETYPE::OTHERS:
                    return "OTHERS";
                case FRAMETYPE::GOAWAY:
                    return "GOAWAY";
            }
        }

} HTTP2Frame;

class HTTP2Parser{
    public:
        HTTP2Parser();
        std::vector<HTTP2Frame> parse(std::span<const char>);
};