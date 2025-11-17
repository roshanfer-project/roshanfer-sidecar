#include "connection_enums.h"
#include "listener.h"
#include "udp_listener.h"
#include "state.h"
#include "ring_helper.hpp"
#include "buffer.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <event_loop.h>
#include <connection.h>
#include <buffer_manager.h>
#include <ring_wrapper.h>
#include <iostream>
#include <memory>
#include <glog/logging.h>
#include <string>
#include <strings.h>



void EventLoop::run() {
    // Pointers to accept and identify completion events
    struct io_uring_cqe *cqe;
    UserData *ud;

    // Add accept submissions
    for (auto& [type, listener] : listeners) {
            VLOG(1) << "Listening on " << listener->type_to_str() << " listener, port: " << listener->get_port();
        ring.prepare_accept(listener, buffer_manager.get_user_data());
    }

    // Add recvmsg submission
    ring.prepare_rcvmsg(
        udp_listener.get_fd(),
        buffer_manager.get_buffer(),
        buffer_manager.get_user_data(),
        UDPType::REQUEST
    );

    // Add recvmsg submission for responses
    ring.prepare_rcvmsg(
        state.get_sockfd(),
        buffer_manager.get_buffer(),
        buffer_manager.get_user_data(),
        UDPType::RESPONSE
    );

    // main event loop
    while(true) {
        ring.submit_and_wait();

        while((cqe = ring.peek_cqe()) != nullptr) {
            VLOG(1) << "Processing completion event";

            // Identify the type of event
            ud = ring.get_user_data(cqe);

            // Handle the event
            switch (ud->op)
            {
            case Operation::ACCEPT: {
                VLOG(1) << "Accept completion event";
                // get the listener from the user data
                    auto listener = ud->listener;

                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to accept connection, error: "
                               << strerror(-cqe->res);
                    //break;
                } else {
                    // add the connection to the listener
                    HTTP http_type;
                    if (config.is_frontend && listener->type == ConnectionType::INGRESS) {
                        http_type = HTTP::HTTP1;
                    } else if (config.is_ingress) {
                        http_type = HTTP::HTTP1;
                    } else {
                        http_type = HTTP::HTTP2;
                    }

                    auto conn = listener->add_connection(
                        cqe->res,
                        std::addressof(rpc_mapper),
                        std::addressof(rpc_queue),
                        http_type,
                        std::addressof(state.stats)
                    );

                    VLOG(1) << "Index:" << index << " accepted connection on " << listener->type_to_str() << " listener"
                            << " fd: " << listener->get_fd();
                    VLOG(1) << "New connection on fd: " << conn->get_fd();

                    // submit the first setting frame
                    conn->submit_settings();

                    // prepare the first read
                    ring.prepare_read(
                        buffer_manager.get_buffer(),
                        buffer_manager.get_user_data(),
                        listener,
                        std::move(conn)
                    );
                }

                // re-arm accept on the listener
                ring.prepare_accept(std::move(listener), buffer_manager.get_user_data());
                break;
            }
            
            case Operation::READ: {
                VLOG(1) << "Read completion event";
                // get the buffer from the user data
                auto buffer = ud->get_buffer();
                if (!buffer) {
                    LOG(FATAL) << "Buffer is null in read completion event";
                }
                
                // get the original connection and listener
                auto orig_conn = ud->conn;
                auto orig_listener = ud->listener;

                // log the read event
                VLOG(1) << "fd: " << orig_conn->get_fd();
                VLOG(1) << "Read " << cqe->res << " bytes from buffer";
                VLOG(1) << "Connection type: " << orig_conn->type_to_str();
                VLOG(1) << "Connection direction: " << orig_conn->direction_to_str();

                // check corner cases (errors, closed connection)
                if (cqe->res <= 0) {
                    if (cqe->res < 0) {
                        state.dump_entire_state();
                        LOG(FATAL) << "Failed to read from " << orig_conn->get_host() << ":" << orig_conn->get_port()
                               << ", error: " << strerror(-cqe->res);
                    } else if (cqe->res == 0) {
                        LOG(INFO) << "Closing connection on fd: " << orig_conn->get_fd() 
                        << " of type: " << orig_conn->type_to_str() 
                        << " and direction: " << orig_conn->direction_to_str();
                        /* if (config.is_ingress && rpc_mapper.check_fd_exists(orig_conn->type, orig_conn->get_fd(), false)) {
                            listeners.at(orig_conn->type)->dump_connections();
                            state.dump_entire_state();
                            LOG(FATAL) << "FD exists in ds map for fd: " << orig_conn->get_fd()
                                       << " of type: " << orig_conn->type_to_str();
                        } */
                    }

                    // update connection status
                    orig_conn->set_status(ConnectionStatus::TEARDOWN);

                    // free buffer
                    buffer_manager.free_buffer(std::move(buffer));

                    // prepare cancel
                    ring.prepare_cancel(
                        std::move(orig_conn),
                        buffer_manager.get_user_data()
                    );
                    break;
                }
                buffer->set_filled((size_t)cqe->res);

                // feed data to nghttp2
                if (!orig_conn) {
                    LOG(FATAL) << "orig_conn is null";
                }
                orig_conn->http_read(buffer, ingress);
                assert(orig_conn != nullptr && "orig_conn should not be null here");

                // send out http2-related data
                state.write_http(orig_conn);

                // free the buffer
                buffer_manager.free_buffer(std::move(buffer));

                // handle req/res send buffers
                state.forward(orig_conn->type, orig_conn->direction);

                // check ingress admission
                if (config.is_ingress) {
                    state.ingress_admit();
                }

                try {
                    state.ppm_client(false, nullptr);
                } catch (const std::out_of_range& e) {
                    LOG(FATAL) << "Out of range error: " << e.what();
                } catch (const std::exception& e) {
                    LOG(FATAL) << "Error in PPM client: " << e.what();
                }
                

                // flush every HTTP2 frame out
                state.write_http(orig_conn);

                // re-arm read
                ring.prepare_read(
                    buffer_manager.get_buffer(),
                    buffer_manager.get_user_data(),
                    std::move(orig_listener),
                    std::move(orig_conn)
                );
                break;
            }

            case Operation::CONNECT: {
                // check if the connection is successful
                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to connect to fd: " << ud->conn->get_fd()
                               << ", error: " << strerror(-cqe->res);
                    break;
                }

                auto orig_conn = ud->conn;

                VLOG(1) << "Connect completion event, fd: " << orig_conn->get_fd();

                // submit first SETTING frame
                orig_conn->submit_settings();
                VLOG(1) << "conn type: " << orig_conn->type_to_str();
                VLOG(1) << "conn direction: " << orig_conn->direction_to_str();

                if (orig_conn->http() == HTTP::HTTP1) {
                    orig_conn->set_status(ConnectionStatus::UP);
                } else if (orig_conn->http() == HTTP::HTTP2) {
                    // write http frames (initial settings, which acts as handshake). Only for HTTP/2 connections
                    state.write_http(orig_conn);
                }
                

                // arm the first read
                ring.prepare_read(
                    buffer_manager.get_buffer(),
                    buffer_manager.get_user_data(),
                    listeners.at(orig_conn->type),
                    orig_conn
                );
                break;
            }

            case Operation::WRITE: {
                VLOG(1) << "Write completion event";
                VLOG(1) << "Wrote " << cqe->res << " bytes to fd: " << ud->conn->get_fd();

                // free buffer
                buffer_manager.free_buffer(ud->get_buffer());

                break;
            }

            case Operation::CANCEL: {
                auto conn = ud->conn;
                VLOG(1) << "Cancel completion event, fd: " << conn->get_fd();

                switch (conn->direction) {
                    case ConnectionDirection::UPSTREAM:
                        // for upstream connections, pools (inside state) hold the connection
                        state.remove_connection(std::move(conn));
                        break;
                    case ConnectionDirection::DOWNSTREAM:
                        // also remove the corresponsing connection from state becasue,
                        // we don't need to route the response to/from this connection
                        /* ring.prepare_cancel(
                            state.get_one_connection(conn.type),
                            buffer_manager.get_user_data()
                        ); */
                        // remove the object of the connection from the listener
                        listeners.at(conn->type)->remove_connection(conn->get_fd());
                        conn.reset();
                        break;
                    default:
                        LOG(FATAL) << "Unknown connection direction";
                }

                break;
            }

            case Operation::RCVMSG: {
                VLOG(1) << "Recvmsg completion event";

                // get the buffer from the user data
                auto old_buffer = ud->get_buffer();
                auto udp_type = ud->udp_type;
                if (cqe->res <= 0) {
                    LOG(FATAL) << "unhandled scenario";
                }
                old_buffer->set_filled((size_t)cqe->res);

                // TODO: check if the received message is a request for QM,
                // or is a response for DN (In the second case, there is no need
                // for replying back).

                switch (udp_type) {
                    case UDPType::REQUEST: {
                        VLOG(1) << "Request for Queue Multiplxer";

                        // get the new buffer from QM
                        auto new_buffer = buffer_manager.get_buffer();
                        state.queue_multiplexer(old_buffer, new_buffer);

                        // prepare the new buffer for sendmsg
                        ring.prepare_reply_sendmsg(
                            udp_listener.get_fd(),
                            old_buffer,
                            std::move(new_buffer),
                            buffer_manager.get_user_data()
                        );

                        // free the old buffer
                        buffer_manager.free_buffer(std::move(old_buffer));

                        // re-arm the recvmsg
                        ring.prepare_rcvmsg(
                            udp_listener.get_fd(),
                            buffer_manager.get_buffer(),
                            buffer_manager.get_user_data(),
                            UDPType::REQUEST
                        );

                        break;
                    }

                    case UDPType::RESPONSE: {
                        VLOG(1) << "Response for DN";

                        // We have a response for DN (potentially a request unblock)
                        try {
                            state.ppm_client(true, old_buffer);
                        } catch (const std::out_of_range& e) {
                            LOG(FATAL) << "Out of range error: " << e.what();
                        } catch (const std::exception& e) {
                            LOG(FATAL) << "Error in PPM client: " << e.what();
                        }
                        

                        // free the buffer
                        buffer_manager.free_buffer(std::move(old_buffer));

                        // re-arm the recvmsg
                        ring.prepare_rcvmsg(
                            state.get_sockfd(),
                            buffer_manager.get_buffer(),
                            buffer_manager.get_user_data(),
                            UDPType::RESPONSE
                        );
                        break;
                    }

                    case UDPType::CLEAR: {
                        LOG(FATAL) << "Received a CLEAR UDPType, this should not happen";
                        break;
                    }
                }
                break;
            }

            case Operation::SENDMSG: {
                VLOG(1) << "Sendmsg completion event";

                if (cqe->res <= 0) {
                    LOG(FATAL) << "Failed to send message"
                               << " res: " << cqe->res
                               << " error: " << strerror(-cqe->res)
                               << " udp_type: " << udp_type_to_str(ud->udp_type);
                }

                // free the buffer
                buffer_manager.free_buffer(ud->get_buffer());

                break;
            }
            
            case Operation::CLEAR: {
                LOG(FATAL) << "Received a CLEAR operation, index: " << ud->index;
                break;
            }

            default:
                LOG(FATAL) << "Unknown operation: " << static_cast<int>(ud->op);
                break;

            }

            // free the user data
            buffer_manager.free_user_data(ud);

            // Advance the ring
            ring.seen_cqe(cqe);
        }
    }
};

EventLoop::EventLoop(int th_index, std::string& ingress_service_ref, Config parsed_config, SharedState& shared_state)
:   index(th_index),
    ingress_service(ingress_service_ref),
    config(parsed_config),
    ring(config.ring_size),
    buffer_manager(config.buffer_count, config.buffer_size),
    listeners(),
    udp_listener(config.ingress_listener_port),
    rpc_mapper(),
    rpc_queue(),
    ingress(config.routing, th_index),
    state(config, ring, buffer_manager, rpc_mapper, rpc_queue, listeners, ingress, shared_state, ingress_service_ref, th_index)
    {   
        if (config.is_ingress) {
            if (!parsed_config.routing.at(ingress_service_ref).ingress_limit.has_value()) {
                LOG(FATAL) << "Ingress limit is not set for ingress service: " << ingress_service_ref;
            }
        }
        
        uint16_t egress_port;
        if (config.is_ingress) {
            egress_port = config.mapping.at(ingress_service_ref).listen_port.value_or(0);
            if (egress_port == 0) {
                LOG(FATAL) << "Egress port is not set for ingress service: " << ingress_service_ref;
            }
        } else {
            egress_port = config.egress_listener_port;
        }
        listeners.emplace(
            ConnectionType::EGRESS,
            std::make_shared<Listener>(egress_port, ConnectionType::EGRESS)
        );

        listeners.emplace(
            ConnectionType::INGRESS,
            std::make_shared<Listener>(config.ingress_listener_port, ConnectionType::INGRESS)
        );
    };