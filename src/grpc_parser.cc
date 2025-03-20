#include "grpc_parser.h"
#include "buffer.h"
#include "connection.h"
#include "http2_parser.h"
#include "glog/logging.h"
#include <memory>

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

gRPCParser::gRPCParser(std::vector<std::unique_ptr<RPCMessage>>& ppm_queue,
    BufferManager& buffer_manager, Stats& stats,
    std::vector<std::unique_ptr<RPCMessage>>& ingress_req_queue)
:   ppm_queue(ppm_queue),
    buffer_manager(buffer_manager),
    stats(stats),
    ingress_req_queue(ingress_req_queue) {}

void gRPCParser::clear(int fd) {
    if (partial_messages.find(fd) != partial_messages.end()) {
        partial_messages.at(fd).clear();
    }
}

inline static void rem_frame_data(std::vector<HTTP2Frame>& frames, HTTP2Frame& frame, Buffer& buffer) {
    int frame_end = frame.offset + frame.length;
    std::memmove(
        buffer.data.get() + frame.offset,
        buffer.data.get() + frame_end,
        buffer.get_filled() - frame_end
    );
    buffer.set_filled(buffer.get_filled() - frame.length);

    // shift the offset of the the following frames
    for (int i = frames.size() - 1; i >= 0; i--) {
        if (frames[i].offset >= frame_end) {
            frames[i].offset -= frame.length;
        }
    }
}

void gRPCParser::parse(HTTPConnection& conn, Buffer& buffer) {
    DLOG(INFO) << "Start gRPC parsing...";

    auto frames = conn.parse(std::span<const char>(buffer.data.get(), buffer.get_filled()));
    bool request = conn.direction == ConnectionDirection::DOWNSTREAM;
    bool ingress = conn.type == ConnectionType::INGRESS;
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

                // copy the request into a new buffer
                Buffer* req_buffer = buffer_manager.get_buffer();
                int write_i = 0;
                for (auto req_frame : req_frames) {
                    std::memcpy(
                        req_buffer->data.get() + write_i,
                        buffer.data.get() + req_frame->offset,
                        req_frame->length
                    );
                    write_i += req_frame->length;
                    rem_frame_data(frames, *req_frame, buffer);
                }
                req_buffer->set_filled(write_i);

                // pushing the complete gRPC message to the right queue
                if (ingress) {
                    ingress_req_queue.push_back(std::make_unique<RPCMessage>(request, req_buffer));
                    ingress_req_queue.back()->set_rcv_time();
                    DLOG(INFO) << "Complete message pushed to ingress request queue";
                } else {
                    ppm_queue.push_back(std::make_unique<RPCMessage>(request, req_buffer));
                    ppm_queue.back()->set_rcv_time();
                    DLOG(INFO) << "Complete message pushed to PPM queue";
                }

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
                msg.set_rcv_time();
                if (ingress) {
                    ingress_req_queue.push_back(std::make_unique<RPCMessage>(msg));
                    DLOG(INFO) << "Complete message (assembled) pushed to ingress request queue";
                } else {
                    ppm_queue.push_back(std::make_unique<RPCMessage>(msg));
                    DLOG(INFO) << "Complete message (assembled) pushed to PPM queue";
                }
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
            rem_frame_data(frames, *frame, buffer);
        }
    }

    DLOG(INFO) << "End gRPC parsing";
}