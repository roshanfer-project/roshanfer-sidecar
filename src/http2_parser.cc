#include "glog/logging.h"
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>
#include "http2_parser.h"


HTTP2Parser::HTTP2Parser(){}

std::vector<HTTP2Frame> HTTP2Parser::parse(std::span<const char> input) {
    std::vector<HTTP2Frame> results;
    int i = 0;
    while(i < input.size()) {
        // accout for preface
        if (i == 0 && input.size() >= 24) {
            if (std::strncmp(input.data() + i, HTTP2_PREFACE, 24) == 0) {
                DLOG(INFO) << "Found preface";
                HTTP2Frame frame;
                frame.type = FRAMETYPE::PREFACE;
                frame.length = 24;
                frame.offset = 0;
                results.push_back(frame);
                i += 24;
                DLOG(INFO) << frame.to_string();
                continue;
            }
        }

        if (input.size() < i + 9) {
            LOG(FATAL) << "incomplete frame, i: " << i << ", size: " << input.size();
        }
        HTTP2Frame frame;
        frame.offset = i;

        // check the length of the payload (first three bytes)    
        frame.length = (input[i] << 16) + (input[i + 1] << 8) + input[i + 2] + 9;
        i += 3;

        // check the type of the frame
        if (input[i] == 0x0) {
            frame.type = FRAMETYPE::DATA;
        } else if (input[i] == 0x1) {
            frame.type = FRAMETYPE::HEADERS;
        } else if (input[i] == 0x4) {
            frame.type = FRAMETYPE::SETTINGS;
        } else if (input[i] == 0x6) {
            frame.type = FRAMETYPE::PING;
        } else if (input[i] == 0x8) {
            frame.type = FRAMETYPE::WINDOW_UPDATE;
        } else if (input[i] == 0x7) {
            frame.type = FRAMETYPE::GOAWAY;
        } else {
            frame.type = FRAMETYPE::OTHERS;
        }
        i += 1;

        // check the flags
        if (frame.type == FRAMETYPE::DATA || frame.type == FRAMETYPE::HEADERS) {
            frame.EOS = (input[i] & 0x01) == 0x01;
            frame.EOH = (input[i] & 0x04) == 0x04;
        }
        i += 1;
        
        // check the stream id
        frame.stream_id = (input[i] << 24) + (input[i + 1] << 16) + (input[i + 2] << 8) + input[i + 3];
        frame.stream_id = frame.stream_id & ~(((uint32_t)1 << 31));  // clear the leftmost bit
        i += 4;

        // check the payload
        if (input.size() < frame.length - 9) {
            LOG(FATAL) << "incomplete payload, length: " << frame.length << ", i: " << i << ", size: " << input.size();
        }
        i += + frame.length - 9;
        results.push_back(frame);
        DLOG(INFO) << frame.to_string();
    }
    
    return results;
}