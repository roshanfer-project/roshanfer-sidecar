#include "state.h"
#include "buffer_manager.h"
#include "config.h"
#include "connection.h"
#include "glog/logging.h"
#include <cstdint>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

State::State(Config config)
:   config(config),
    queues(),
    pools(),
    metrics(),
    ppm_state() {
        pools.emplace(ConnectionType::EGRESS, ConnectionPool(ConnectionType::EGRESS));
        pools.emplace(ConnectionType::INGRESS, ConnectionPool(ConnectionType::INGRESS));
        queues.emplace(ConnectionType::EGRESS, std::vector<Buffer*>());
        queues.emplace(ConnectionType::INGRESS, std::vector<Buffer*>());
}


int State::route(ConnectionType type) {
    try {
        auto& conn = pools.at(type).get_any_connection();
        if (conn->get_status() == ConnectionStatus::DOWN) {
            throw ConnectionNotUPException(conn);
        }
        return conn->get_fd();
    } catch (NoConnectionException& e) {
        LOG(INFO) << "No connection available. Starting a new connection.";

        std::string host;
        int port;
        if (type == ConnectionType::EGRESS) {
            host = config.endpoint_host;
            port = config.endpoint_port;
        } else if (type == ConnectionType::INGRESS) {
            host = config.ingress_upstream_host;
            port = config.ingress_upstream_port;
        } else {
            LOG(FATAL) << "Unknown connection type";
        }
        std::unique_ptr<HTTPConnection>& conn = pools.at(type).add_connection(
            host, port);
        DLOG(INFO) << "New connection established on fd: " << conn->get_fd();
        throw AddConnectionException(conn);
    }
}

void State::add_buffer(Buffer* buffer, ConnectionType type) {
    queues[type].push_back(buffer);
}

bool State::has_buffer(ConnectionType type) {
    return !queues[type].empty();
}

Buffer* State::get_buffer(ConnectionType type) {
    Buffer* buffer;
    buffer = queues[type].front();
    queues[type].erase(queues[type].begin());
    return buffer;
}

std::unique_ptr<HTTPConnection>& State::get_connection(int fd, ConnectionType type) {
    return  pools.at(type).get_connection(fd);
}

void State::remove_one_connection(ConnectionType type) {
    pools.at(type).remove_connection(pools.at(type).get_any_connection()->get_fd());
}

HTTPConnection& State::get_one_connection(ConnectionType type) {
    return *pools.at(type).get_any_connection().get();
}

void State::remove_connection(int fd, ConnectionType type) {
    pools.at(type).remove_connection(fd);
}

void State::update_state(HTTPConnection& conn, Buffer* buffer) {
    auto frames = conn.parse(std::span<const char>(buffer->data.get(), buffer->get_filled()));

    /*
    check if we need to even do anything (we only care about requests or responses
    coming out of local app). In the case of requests, we need to run PPM client logic
    and for responses we need to update Metrics.
    */

    if (conn.type == ConnectionType::EGRESS) {
        // analyze the frames and buffer incomplete requests
        auto [complete_messages, incomplete_frames] = analyze_messages(frames, true);
        DLOG(INFO) << "Complete messages: " << complete_messages.size();
        DLOG(INFO) << "Incomplete frames: " << incomplete_frames.size();

        // remove incomplete requests from the Buffer
        if (incomplete_frames.size() > 0) {
            LOG(FATAL) << "Incomplete requests found. "
                << "You have not implemented memory management for incomplete requests.";
        }
        // run the PPM client logic
        

    } else if (conn.type == ConnectionType::INGRESS
        && conn.direction == ConnectionDirection::UPSTREAM) {
        // analyze the frames but DO NOT buffer incomplete resppnses
        auto [complete_messages, incomplete_frames] = analyze_messages(frames, false);
        DLOG(INFO) << "Complete messages: " << complete_messages.size();
        DLOG(INFO) << "Incomplete frames: " << incomplete_frames.size();

        if (incomplete_frames.size() > 0) {
            LOG(FATAL) << "Incomplete requests found. "
                << "You have not implemented memory management for incomplete requests.";
        }

        // update metrics only based on complete responses
        metrics.add_resp_out(complete_messages.size());
    }
}

std::tuple<std::unordered_map<uint32_t, RPCMessage>, std::vector<HTTP2Frame>> 
State::analyze_messages(std::vector<HTTP2Frame>& frames, bool request) {

    // temporary representation for incomplete streams
    std::unordered_map<uint32_t, uint8_t> inc_stream_map;
    // temporary representation for complete streams
    std::unordered_map<uint32_t, uint8_t> c_stream_map;

    std::unordered_map<uint32_t, RPCMessage> complete_messages;
    std::vector<HTTP2Frame> incomplete_frames;

    /*
        Go over frames and fill two maps for incomplete and complete messages.
    */
    for (auto& frame : frames) {
        if (request) {
            // we are looking for requests
            if (frame.type == FRAMETYPE::HEADERS) {
                if (frame.EOH == 0) {
                    LOG(FATAL) << "Multi-frame HEADERS are not supported for requests";
                }
                // check if stream is present
                if (inc_stream_map.find(frame.stream_id) != inc_stream_map.end()) {
                    LOG(FATAL) << "HEADERS frame already present";
                }
                inc_stream_map[frame.stream_id] = 1;
            } else if (frame.type == FRAMETYPE::DATA) {
                if (frame.EOS == 0) {
                    LOG(FATAL) << "Multi-frame DATA are not supported for requests";
                }
                
                // check if the corresponding HEADERS frame is present
                if (inc_stream_map.find(frame.stream_id) == inc_stream_map.end()) {
                    LOG(FATAL) << "DATA frame without HEADERS frame";
                } else if (inc_stream_map[frame.stream_id] == 1) {
                    // remove the stream from the incomplete map
                    inc_stream_map.erase(frame.stream_id);
                    // add to the complete map
                    c_stream_map[frame.stream_id] = 1;
                    DLOG(INFO) << "Complete message with stream_id: " << frame.stream_id;
                }
            }
        } 
        else {
            if (frame.type == FRAMETYPE::HEADERS) {
                if (frame.EOH == 0) {
                    LOG(FATAL) << "Multi-frame HEADERS are not supported for responses";
                }

                if (frame.EOS == 0) {
                    // check if stream is present
                    if (inc_stream_map.find(frame.stream_id) != inc_stream_map.end()) {
                        LOG(FATAL) << "Out of order HEADERS frame";
                    }
                    inc_stream_map[frame.stream_id] = 1;
                } else if (frame.EOS == 1) {
                    if (inc_stream_map[frame.stream_id] < 2) {
                        LOG(FATAL) << "Out of order tailers HEADERS frame";
                    } else if (inc_stream_map[frame.stream_id] > 2) {
                        LOG(FATAL) << "Mutli-frame DATA are not supported for responses";
                    }
                    inc_stream_map.erase(frame.stream_id);
                    c_stream_map[frame.stream_id] = 1;
                    DLOG(INFO) << "Complete message with stream_id: " << frame.stream_id;
                }
            } else if (frame.type == FRAMETYPE::DATA) {
                if (inc_stream_map.find(frame.stream_id) == inc_stream_map.end()) {
                    LOG(FATAL) << "DATA frame without HEADERS frame";
                } else if (inc_stream_map[frame.stream_id] == 1) {
                    inc_stream_map[frame.stream_id] = 2;
                } else {
                    LOG(FATAL) << "Out of order DATA frame";
                }
            }
        }
    }

    /*
        Based on two maps produced above, do these:
        1. Add incomplete messages to the `inc_request_buffer`
        2. If doing step 1 completes any messages, add them to `complete_messages`
        3. If there are any incomplete messages, add them to `incomplete_frames`
    */
    if (request) {
        /////// update the internal buffer with incomplete messages

        // check if we have any incomplete messages
        if (inc_stream_map.size() > 0) {
            LOG(FATAL) << "Incomplete messages found. "
                << "You have not implemented memory management for incomplete messages.";
            /*
            TODO: Suppose that you have a Buffer that has some incomplete messages.
            If these incomplete messages remain incomplete after this function, it's fine
            because the higher level function will just remove them from the Buffer and
            store it internally. However, if they get completed in this function, you
            need a more expressive way of returning to the higher level function because some
            part of the complete message exissts in the Buffer but some part not!
            */
            
            /* for (auto& frame : frames) {
                if (inc_stream_map.find(frame.stream_id) != inc_stream_map.end()) {
                    // add the frame to the buffer
                    if (inc_request_buffer.find(frame.stream_id) == inc_request_buffer.end()) {
                        inc_request_buffer.emplace(frame.stream_id, RPCMessage(GRPCMESSAGE::REQUEST));
                    }
                    if (inc_request_buffer.at(frame.stream_id).add_frame(frame)) {
                        // we have a complete message
                        complete_messages.emplace(frame.stream_id, inc_request_buffer[frame.stream_id]);
                        inc_request_buffer.erase(frame.stream_id);

                        inc_stream_map.erase(frame.stream_id);
                    }
                }
            } */
        }

        // preprae a list of newly completed messages and new incomplete messages
        if (inc_stream_map.size() > 0) {
            LOG(FATAL) << "Incomplete messages found. "
                << "You have not implemented memory management for incomplete messages.";
            /* for (auto& frame : frames) {
                if (inc_stream_map.find(frame.stream_id) != inc_stream_map.end()) {
                    incomplete_frames.push_back(frame);
                }
            } */
        }

        if (c_stream_map.size() > 0) {
            for (auto& frame : frames) {
                if (c_stream_map.find(frame.stream_id) != c_stream_map.end()) {
                    if (complete_messages.find(frame.stream_id) == complete_messages.end()) {
                        complete_messages.emplace(frame.stream_id, RPCMessage(GRPCMESSAGE::REQUEST));
                    }
                    complete_messages.at(frame.stream_id).add_frame(frame);
                }
            }
        }
    } else {
        if (inc_stream_map.size() > 0) {
            LOG(FATAL) << "Logic for incomplete responses not implemented";
        }

        if (c_stream_map.size() > 0) {
            for (auto& frame : frames) {
                if (c_stream_map.find(frame.stream_id) != c_stream_map.end()) {
                    if (complete_messages.find(frame.stream_id) == complete_messages.end()) {
                        complete_messages.emplace(frame.stream_id, RPCMessage(GRPCMESSAGE::RESPONSE));
                    }
                    complete_messages.at(frame.stream_id).add_frame(frame);
                }
            }
        }
    }

    return std::make_tuple(complete_messages, incomplete_frames);
};

Metrics::Metrics(): resp_out(0) {}

void Metrics::add_resp_out(int inc) {
    resp_out += inc;
}

int Metrics::get_resp_out() {
    return resp_out;
}

RPCMessage::RPCMessage(GRPCMESSAGE type): frames(), type(type) {}

bool RPCMessage::add_frame(HTTP2Frame& frame) {
    frames.push_back(frame);
    if (type == GRPCMESSAGE::REQUEST) {
        return  frames.size() == 2;
    } else {
        return frames.size() == 3;
    }
}

PPMState::PPMState(): sent_credits(0) {}

inline static void write_dn_response(int result, Buffer* resp) {
    resp->data.get()[0] = 0x01;
    resp->data.get()[1] = 0x01;
    resp->data.get()[2] = result;
    resp->set_filled(3);
}

void State::queue_multiplexer(Buffer* req, Buffer* resp) {
    // read the request
    if (req->data.get()[0] == 0x01) {
        // we have demand notification

        // check if it's a request
        if (req->data.get()[1] != 0x00) {
            LOG(FATAL) << "QM only handles DN requests";
        }

        // produce the response
        uint8_t result = 0;
        if (config.ppm_limit > ppm_state.sent_credits - metrics.get_resp_out()) {
            result = 1;
            ppm_state.sent_credits++;
        }

        // write the response
        write_dn_response(result, resp);
    } else {
        LOG(FATAL) << "Unknown message type";
    }
}