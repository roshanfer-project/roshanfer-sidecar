#include "rpc_message.h"
#include "http2_parser.h"
#include "buffer.h"
#include <cstring>

RPCMessage::RPCMessage(bool request, Buffer* buffer)
    : frames(), request(request), buffer(buffer) {}


bool RPCMessage::add_frame(HTTP2Frame frame, char data[]) {
    frames.push_back(frame);
    length += frame.length;

    if (request) {
        std::memcpy(
            buffer->data.get() + buffer->get_filled(),
            data,
            frame.length
        );
        buffer->set_filled(buffer->get_filled() + frame.length);
    }
    
    if (request) {
        return frame.EOS == 1 && frame.type == FRAMETYPE::DATA
        && frames.size() == 2;
    } else {
        return frame.EOS == 1 && frame.type == FRAMETYPE::HEADERS
        && frames.size() == 3;
    }
}

void RPCMessage::set_rcv_time() {
    rcv_time = std::chrono::system_clock::now();
}