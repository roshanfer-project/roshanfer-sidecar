#include "grpc_parser.h"
#include "connection.h"
#include "http2_parser.h"
#include "glog/logging.h"

LocalgRPCParser::LocalgRPCParser(bool request)
: map(), request(request) {}

bool LocalgRPCParser::add_frame(HTTP2Frame& frame) {
    // check if stream is present
    if (map.find(frame.stream_id) == map.end()) {
        map.emplace(frame.stream_id, std::vector<HTTP2Frame*>());
    }

    if (frame.type == FRAMETYPE::HEADERS) {
        if (frame.EOH == 0) {
            LOG(FATAL) << "Multi-frame HEADERS are not supported for requests";
        }
    }

    map.at(frame.stream_id).push_back(&frame);

    if (request) {
        return frame.EOS == 1 && frame.type == FRAMETYPE::DATA
        && map.at(frame.stream_id).size() == 2;
    } else {
        return frame.EOS == 1 && frame.type == FRAMETYPE::HEADERS
        && map.at(frame.stream_id).size() == 3;
    }
};

std::vector<HTTP2Frame*> LocalgRPCParser::get(uint32_t stream_id) {
    auto result = map.at(stream_id);
    // check if the message is contigous
    for (int i = 0; i < result.size() - 1; i++) {
        if (result[i]->offset + result[i]->length != result[i + 1]->offset) {
            LOG(FATAL) << "Only contiguous frames are supported";
        }
    }
    map.erase(stream_id);
    return result;
};

std::vector<HTTP2Frame*> LocalgRPCParser::get_all() {
    std::vector<HTTP2Frame*> result;
    for (auto& [stream_id, frames] : map) {
        for (auto frame : frames) {
            result.push_back(frame);
        }
    }
    map.clear();
    return result;
};

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

gRPCParser::gRPCParser(std::vector<Buffer*>& ppm_queue, BufferManager& buffer_manager,
        Stats& stats)
: ppm_queue(ppm_queue), buffer_manager(buffer_manager), stats(stats) {}

void gRPCParser::clear(int fd) {
    if (partial_messages.find(fd) != partial_messages.end()) {
        partial_messages.at(fd).clear();
    }
}

void gRPCParser::parse(HTTPConnection& conn, Buffer& buffer) {
    if (conn.direction == ConnectionDirection::DOWNSTREAM
    && conn.type == ConnectionType::INGRESS) {
        // we are not interested in ingress downstream requests
        return;
    }

    DLOG(INFO) << "Start gRPC parsing...";

    auto frames = conn.parse(std::span<const char>(buffer.data.get(), buffer.get_filled()));
    bool request = conn.direction == ConnectionDirection::DOWNSTREAM;
    auto local_parser = LocalgRPCParser(request);

    for (auto& frame : frames) {
        // we are only interested in HEADERS and DATA frames
        if (frame.type != FRAMETYPE::HEADERS && frame.type != FRAMETYPE::DATA) {
            continue;
        }

        if (local_parser.add_frame(frame)) {
            // buffer contains a complete message
            if (request) {
                auto req_frames = local_parser.get(frame.stream_id);
                // copy the request from the buffer and store it in ppm queue
                Buffer* req_buffer = buffer_manager.get_buffer();
                int write_i = 0;
                for (auto req_frame : req_frames) {
                    std::memcpy(
                        req_buffer->data.get() + write_i,
                        buffer.data.get() + req_frame->offset,
                        req_frame->length
                    );
                    write_i += req_frame->length;
                }
                req_buffer->set_filled(write_i);

                // pushing the complete gRPC message to the ppm queue
                ppm_queue.push_back(req_buffer);
                DLOG(INFO) << "Complete message pushed to PPM queue";

                // remove the request frames from the buffer
                int msg_start = req_frames.front()->offset;
                int msg_end = req_frames.back()->offset + req_frames.back()->length;

                // move the remaining data to the start of the buffer
                std::memmove(
                    buffer.data.get() + msg_start,
                    buffer.data.get() + msg_end,
                    buffer.get_filled() - msg_end
                );
                buffer.set_filled(buffer.get_filled() - (msg_end - msg_start));
            } else {
                local_parser.remove(frame.stream_id);
                if (conn.type == ConnectionType::EGRESS) {
                    stats.sidecar_resp_in = true;
                    DLOG(INFO) << "Complete response received by sidecar";
                } else if (conn.type == ConnectionType::INGRESS) {
                    stats.app_resp_out += 1;
                    DLOG(INFO) << "Incremented app responce out";
                }
            }
        }
    }

    // transfer incomplete messages to global parser
    auto& inc_messages = partial_messages[conn.get_fd()];
    auto incomplete_frames = local_parser.get_all();
    for (auto frame : incomplete_frames) {
        if (inc_messages.find(frame->stream_id) == inc_messages.end()) {
            inc_messages.emplace(frame->stream_id, RPCMessage(request, buffer_manager.get_buffer()));
        }
        // get the chunk if buffer holding the frame
        if (inc_messages.at(frame->stream_id).add_frame(*frame,
             buffer.data.get()+frame->offset)) {
            DLOG(INFO) << "Assembled a message";
            if (request) {
                auto msg = inc_messages.at(frame->stream_id);
                inc_messages.erase(frame->stream_id);
                ppm_queue.push_back(msg.get_buffer());
                DLOG(INFO) << "Complete message (assembled) pushed to PPM queue";
            } else {
                if (conn.type == ConnectionType::EGRESS) {
                    stats.sidecar_resp_in = true;
                    DLOG(INFO) << "Complete response (assembled) received by sidecar";
                } else if (conn.type == ConnectionType::INGRESS) {
                    stats.app_resp_out += 1;
                    DLOG(INFO) << "Incremented app responce out (assembled)"; 
                }
            }
        }

        if (request) {
            DLOG(INFO) << "Remove from buffer: " << frame->to_string();
            // remove the frame from the buffer
            int frame_start = frame->offset;
            int frame_end = frame_start + frame->length;
            std::memmove(
                buffer.data.get() + frame_start,
                buffer.data.get() + frame_end,
                buffer.get_filled() - frame_end
            );
            buffer.set_filled(buffer.get_filled() - (frame_end - frame_start));
        }
    }

    DLOG(INFO) << "End gRPC parsing";
}