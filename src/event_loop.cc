#include "connection_enums.h"
#include "listener.h"
#include "udp_listener.h"
#include "state.h"
#include "ring_helper.h"
#include "buffer.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <event_loop.h>
#include <connection.h>
#include <buffer_manager.h>
#include <ring_wrapper.h>
#include <iostream>
#include <memory>
#include <glog/logging.h>



void EventLoop::run() {
    // Pointers to accept and identify completion events
    struct io_uring_cqe *cqe;
    struct UserData *ud;

    // Add accept submissions
    for (auto& listener : listeners) {
        VLOG(1) << "Listening on " << listener.second.type_to_str() << " listener, port: " << listener.second.get_port();
        ring.prepare_accept(listener.second, buffer_manager.get_user_data());
    }

    // Add recvmsg submission
    ring.prepare_rcvmsg(
        udp_listener.get_fd(),
        buffer_manager.get_buffer(),
        buffer_manager.get_user_data(),
        UDPType::REQUEST
    );

    // main event loop
    while(true) {
        ring.submit_and_wait();

        while((cqe = ring.peek_cqe())) {
            VLOG(1) << "Processing completion event";

            // Identify the type of event
            ud = ring.get_user_data(cqe);

            // Handle the event
            switch (ud->op)
            {
            case Operation::ACCEPT: {
                VLOG(1) << "Accept completion event";

                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to accept connection, error: "
                               << strerror(-cqe->res);
                    break;
                }

                // get the listener from the user data
                Listener* listener = ud->listener;

                // add the connection to the listener
                HTTPConnection& conn = listener->add_connection(
                    cqe->res,
                    std::addressof(rpc_mapper),
                    std::addressof(rpc_queue)
                );

                VLOG(1) << "Accepted connection on " << listener->type_to_str() << " listener"
                           << " fd: " << listener->get_fd();
                VLOG(1) << "New connection on fd: " << conn.get_fd();

                // submit the first setting frame
                conn.submit_settings();

                // prepare the first read
                Buffer* buffer = buffer_manager.get_buffer();
                auto read_ud = buffer_manager.get_user_data();
                prepare_read(read_ud, buffer, listener, std::addressof(conn));
                ring.prepare_read(
                    buffer,
                    conn.get_fd(),
                    read_ud
                );

                // re-arm accept on the listener
                ring.prepare_accept(*listener, buffer_manager.get_user_data());
                break;
            }
            
            case Operation::READ: {
                // get the buffer from the user data
                Buffer* buffer = ud->buffer;
                buffer->set_filled(cqe->res);

                // get the original connection and listener
                auto orig_conn = ud->conn;
                auto orig_listener = ud->listener;

                // log the read event
                VLOG(1) << "Read completion event, fd: " << orig_conn->get_fd();
                VLOG(1) << "Read " << cqe->res << " bytes from buffer: " << buffer->data.get();
                VLOG(1) << "Connection type: " << orig_conn->type_to_str();
                VLOG(1) << "Connection direction: " << orig_conn->direction_to_str();

                // check corner cases (errors, closed connection)
                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to read from fd: " << orig_conn->get_fd()
                               << ", error: " << strerror(-cqe->res);
                } else if (cqe->res == 0) {
                    LOG(INFO) << "Closing connection on fd: " << orig_conn->get_fd();

                    // update connection status
                    orig_conn->set_status(ConnectionStatus::TEARDOWN);

                    // free buffer
                    buffer_manager.free_buffer(buffer);

                    // prepare cancel
                    ring.prepare_cancel(
                        *orig_conn,
                        buffer_manager.get_user_data()
                    );
                    break;
                }

                // feed data to nghttp2
                orig_conn->http_read(buffer);

                // send out http2-related data
                state.write_http(orig_conn);

                // free the buffer
                buffer_manager.free_buffer(buffer);

                // handle req/res send buffers
                state.forward(orig_conn->type, orig_conn->direction);

                if (orig_conn->type == ConnectionType::EGRESS && orig_conn->direction == ConnectionDirection::DOWNSTREAM) {
                    state.ppm_client(false, nullptr);
                }

                // flush every HTTP2 frame out
                state.write_http(orig_conn);

                // re-arm read
                buffer = buffer_manager.get_buffer();
                auto read_ud = buffer_manager.get_user_data();
                prepare_read(read_ud, buffer, orig_listener, orig_conn);
                ring.prepare_read(
                    buffer,
                    orig_conn->get_fd(),
                    read_ud
                );
                break;
            }

            case Operation::CONNECT: {
                // check if the connection is successful
                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to connect to fd: " << ud->conn->get_fd();
                    break;
                }

                HTTPConnection* orig_conn = ud->conn;

                VLOG(1) << "Connect completion event, fd: " << orig_conn->get_fd();

                // submit first SETTING frame
                orig_conn->submit_settings();
                VLOG(1) << "conn type: " << orig_conn->type_to_str();
                VLOG(1) << "conn direction: " << orig_conn->direction_to_str();

                // write http frames
                state.write_http(orig_conn);

                // arm the first read
                Buffer* read_buffer = buffer_manager.get_buffer();
                auto read_ud = buffer_manager.get_user_data();
                prepare_read(read_ud, read_buffer, std::addressof(listeners.at(orig_conn->type)), orig_conn);
                ring.prepare_read(
                    read_buffer,
                    orig_conn->get_fd(),
                    read_ud
                );
                break;
            }

            case Operation::WRITE: {
                VLOG(1) << "Write completion event";
                VLOG(1) << "Wrote " << cqe->res << " bytes to fd: " << ud->conn->get_fd();

                // free buffer
                buffer_manager.free_buffer(ud->buffer);

                break;
            }

            case Operation::CANCEL: {
                HTTPConnection* conn = ud->conn;
                VLOG(1) << "Cancel completion event, fd: " << conn->get_fd();

                switch (conn->direction) {
                    case ConnectionDirection::UPSTREAM:
                        // for upstream connections, pools (inside state) hold the connection
                        state.remove_connection(*conn);
                        break;
                    case ConnectionDirection::DOWNSTREAM:
                        // also remove the corresponsing connection from state becasue,
                        // we don't need to route the response to/from this connection
                        /* ring.prepare_cancel(
                            state.get_one_connection(conn.type),
                            buffer_manager.get_user_data()
                        ); */
                        // remove the object of the connection from the listener
                        listeners.at(conn->type).remove_connection(conn->get_fd());
                        break;
                    default:
                        LOG(FATAL) << "Unknown connection direction";
                }

                break;
            }

            case Operation::RCVMSG: {
                VLOG(1) << "Recvmsg completion event";

                // get the buffer from the user data
                Buffer* old_buffer = ud->buffer;
                auto udp_type = ud->udp_type;
                old_buffer->set_filled(cqe->res);

                // TODO: check if the received message is a request for QM,
                // or is a response for DN (In the second case, there is no need
                // for replying back).

                switch (udp_type) {
                    case UDPType::REQUEST: {
                        VLOG(1) << "Request for Queue Multiplxer";

                        // get the new buffer from QM
                        Buffer* new_buffer = buffer_manager.get_buffer();
                        state.queue_multiplexer(old_buffer, new_buffer);

                        // prepare the new buffer for sendmsg
                        ring.prepare_reply_sendmsg(
                            udp_listener.get_fd(),
                            old_buffer,
                            new_buffer,
                            buffer_manager.get_user_data()
                        );

                        // free the old buffer
                        buffer_manager.free_buffer(old_buffer);

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

                        Buffer* buffer = ud->buffer;

                        // We have a response for DN (potentially a request unblock)
                        state.ppm_client(true, buffer);

                        // free the buffer
                        buffer_manager.free_buffer(buffer);

                        // Note that there is no need to re-arm this operation becase
                        // everytime we send a DN request we also post a recvmsg sqe
                        break;
                    }
                }
                break;
            }

            case Operation::SENDMSG: {
                VLOG(1) << "Sendmsg completion event";

                // get the buffer from the user data
                Buffer* buffer = ud->buffer;

                // free the buffer
                buffer_manager.free_buffer(buffer);

                break;
            }
            
            default:
                break;
            }

            // free the user data
            buffer_manager.free_user_data(ud);

            // Advance the ring
            ring.seen_cqe(cqe);
        }
    }
};

EventLoop::EventLoop(Config config)
:   ring(config.ring_size),
    buffer_manager(config.buffer_count, config.buffer_size),
    state(config, ring, buffer_manager, rpc_mapper, rpc_queue, listeners),
    listeners(),
    udp_listener(config.ingress_listener_port),
    rpc_mapper(),
    rpc_queue()
    {
        listeners.emplace(
            ConnectionType::EGRESS,
            Listener(config.egress_listener_port, ConnectionType::EGRESS)
        );

        if (!config.disable_ingress) {
            listeners.emplace(
                ConnectionType::INGRESS,
                Listener(config.ingress_listener_port, ConnectionType::INGRESS)
            );
        }
    };