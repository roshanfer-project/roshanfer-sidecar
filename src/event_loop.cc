#include "listener.h"
#include "udp_listener.h"
#include "state.h"
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
        DLOG(INFO) << "Listening on " << listener.second.type_to_str() << " listener, port: " << listener.second.get_port();
        ring.prepare_accept(listener.second, buffer_manager.get_user_data());
    }

    // Add recvmsg submission
    ring.prepare_rcvmsg(
        udp_listener.get_fd(),
        buffer_manager.get_buffer(),
        buffer_manager.get_user_data()
    );

    // main event loop
    while(true) {
        ring.submit_and_wait();

        while((cqe = ring.peek_cqe())) {
            DLOG(INFO) << "Processing completion event";

            // Identify the type of event
            ud = ring.get_user_data(cqe);

            // Handle the event
            switch (ud->op)
            {
            case ACCEPT: {
                DLOG(INFO) << "Accept completion event";

                // get the listener from the user data
                Listener* listener = reinterpret_cast<Listener*>(ud->data);

                // add the connection to the listener
                HTTPConnection& conn = listener->add_connection(cqe->res);

                DLOG(INFO) << "Accepted connection on " << listener->type_to_str() << " listener"
                           << " fd: " << listener->get_fd();
                DLOG(INFO) << "New connection on fd: " << conn.get_fd();

                // get buffer and prepare it
                Buffer* buffer = buffer_manager.get_buffer();
                buffer->prepare_read(std::addressof(conn), listener);

                // prepare ring for read on the newly accepted connection
                ring.prepare_read(
                    buffer,
                    conn.get_fd(),
                    buffer_manager.get_user_data()
                );

                // re-arm accept on the listener
                ring.prepare_accept(*listener, buffer_manager.get_user_data());
                break;
            }
            
            case READ: {
                // get the buffer from the user data
                Buffer* buffer = reinterpret_cast<Buffer*>(ud->data);
                buffer->set_filled(cqe->res);

                // get the original connection and listener
                auto& orig_conn = buffer->get_conn();
                auto& orig_listener = buffer->get_listener();

                // log the read event
                DLOG(INFO) << "Read completion event, fd: " << orig_conn.get_fd();
                DLOG(INFO) << "Read " << cqe->res << " bytes from buffer: " << buffer->data.get();
                DLOG(INFO) << "Connection type: " << orig_conn.type_to_str();
                DLOG(INFO) << "Connection direction: " << orig_conn.direction_to_str();
                if (orig_conn.direction == ConnectionDirection::UPSTREAM) {
                    DLOG(INFO) << "Message type: Response";
                } else if (orig_conn.direction == ConnectionDirection::DOWNSTREAM) {
                    DLOG(INFO) << "Message type: Request";
                }

                // check corner cases (errors, closed connection)
                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to read from fd: " << orig_conn.get_fd();
                } else if (cqe->res == 0) {
                    LOG(INFO) << "Closing connection on fd: " << orig_conn.get_fd();

                    // update connection status
                    orig_conn.set_status(ConnectionStatus::TEARDOWN);

                    // free buffer
                    buffer_manager.free_buffer(buffer);

                    // prepare cancel
                    ring.prepare_cancel(
                        orig_conn,
                        buffer_manager.get_user_data()
                    );
                    break;
                }
                
                state.update_state(orig_conn, buffer);

                if (orig_conn.direction == ConnectionDirection::DOWNSTREAM) {

                    // pick (or create) an upstream connection
                    try {
                        int dst_fd = state.route(orig_conn.type);
                        DLOG(INFO) << "Routing to fd: " << dst_fd;
                        
                        // prepare buffer for write
                        buffer->prepare_write(state.get_connection(dst_fd, orig_conn.type).get());

                        // prepare write (to write the request)
                        ring.prepare_write(
                            dst_fd,
                            buffer,
                            buffer_manager.get_user_data()
                        );

                    } catch (AddConnectionException& e) {
                        DLOG(INFO) << "Connection not available. Waiting for connection to be UP";
                        // prepare connect
                        ring.prepare_connect(
                            e.conn,
                            buffer_manager.get_user_data()
                        );

                        // pass buffer to the state
                        state.add_buffer(buffer, orig_conn.type);
                    } catch (ConnectionNotUPException& e) {
                        DLOG(INFO) << "Connection not UP. Waiting for connection to be UP";
                        // just queue the buffer in state
                        state.add_buffer(buffer, orig_conn.type);
                    }
                } else if (orig_conn.direction == ConnectionDirection::UPSTREAM) {
                    // TODO: if we have multiple downstream connections, we need to route
                    // the response to the correct connection

                    if (listeners.at(orig_conn.type).no_connections()) {
                        LOG(WARNING) << "No " << listeners.at(orig_conn.type).type_to_str() << " connections available";
                        // free buffer
                        buffer_manager.free_buffer(buffer);
                        break;
                    } else {
                        HTTPConnection* downstream_conn = 
                            listeners.at(orig_conn.type).get_connections().begin()->second.get();

                        if (downstream_conn->get_status() == ConnectionStatus::TEARDOWN) {
                            LOG(WARNING) << "target DOWNSTREAM connection is closed";
                            // free buffer
                            buffer_manager.free_buffer(buffer);
                            break;
                        } else {
                            buffer->prepare_write(downstream_conn);

                            // prepare write (to write the response)
                            ring.prepare_write(
                                downstream_conn->get_fd(),
                                buffer,
                                buffer_manager.get_user_data()
                            );
                        }
                    }
                }
                

                // get a new buffer for read and prepare it
                buffer = buffer_manager.get_buffer();
                buffer->prepare_read(std::addressof(orig_conn), std::addressof(orig_listener));

                // re-arm read
                ring.prepare_read(
                    buffer,
                    orig_conn.get_fd(),
                    buffer_manager.get_user_data()
                );
                break;
            }

            case CONNECT: {
                HTTPConnection* orig_conn = reinterpret_cast<HTTPConnection*>(ud->data);
                orig_conn->set_status(ConnectionStatus::UP);

                DLOG(INFO) << "Connect completion event, fd: " << orig_conn->get_fd();

                while (state.has_buffer(orig_conn->type)) {
                    // update connection in buffer
                    Buffer* buffer = state.get_buffer(orig_conn->type);

                    DLOG(INFO) << "writing " << buffer->get_filled() 
                        << " bytes to fd: " << orig_conn->get_fd();
                    
                    // preprare buffer for the write
                    buffer->prepare_write(orig_conn);

                    // prepare write (to write the request)
                    ring.prepare_write(
                        orig_conn->get_fd(),
                        buffer,
                        buffer_manager.get_user_data()
                    );

                    // arm the first read for this newly connected connection
                    Buffer* read_buffer = buffer_manager.get_buffer();
                    // for RESPONSE reads we don't need the listener in the case
                    // that we need to close the connection (we do it from connection pool)
                    read_buffer->prepare_read(
                        orig_conn,
                        std::addressof(listeners.at(orig_conn->type))
                    );

                    // prepare read
                    ring.prepare_read(
                        read_buffer,
                        orig_conn->get_fd(),
                        buffer_manager.get_user_data()
                    );
                }
                break;
            }

            case WRITE: {
                DLOG(INFO) << "Write completion event";
                Buffer* buffer = reinterpret_cast<Buffer*>(ud->data);
                HTTPConnection& conn = buffer->get_conn();

                DLOG(INFO) << "Wrote " << cqe->res << " bytes to fd: " << conn.get_fd();

                // free buffer
                buffer_manager.free_buffer(buffer);
                break;
            }

            case CANCEL: {
                HTTPConnection& conn = *reinterpret_cast<HTTPConnection*>(ud->data);
                DLOG(INFO) << "Cancel completion event, fd: " << conn.get_fd();

                switch (conn.direction) {
                    case ConnectionDirection::UPSTREAM:
                        // for upstream connections, pools (inside state) hold the connection
                        state.remove_connection(conn.get_fd(), conn.type);
                        break;
                    case ConnectionDirection::DOWNSTREAM:
                        // also remove the corresponsing connection from state becasue,
                        // we don't need to route the response to this connection
                        ring.prepare_cancel(
                            state.get_one_connection(conn.type),
                            buffer_manager.get_user_data()
                        );
                        // remove the object of the connection from the listener
                        listeners.at(conn.type).remove_connection(conn.get_fd());
                        break;
                    default:
                        LOG(FATAL) << "Unknown connection direction";
                }

                break;
            }

            case RCVMSG: {
                DLOG(INFO) << "Recvmsg completion event";

                // get the buffer from the user data
                Buffer* old_buffer = reinterpret_cast<Buffer*>(ud->data);
                old_buffer->set_filled(cqe->res);

                // run the Queue Multiplxer logic
                DLOG(INFO) << "Queue Multiplxer logic";

                // get the new buffer from QM
                Buffer* new_buffer = buffer_manager.get_buffer();
                state.queue_multiplexer(old_buffer, new_buffer);

                // prepare the new buffer for sendmsg
                ring.prepare_sendmsg(
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
                    buffer_manager.get_user_data()
                );

                break;
            }

            case SENDMSG: {
                DLOG(INFO) << "Sendmsg completion event";

                // get the buffer from the user data
                Buffer* buffer = reinterpret_cast<Buffer*>(ud->data);

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
    state(config),
    listeners(),
    udp_listener(config.ingress_listener_port) {
        listeners.emplace(
            ConnectionType::EGRESS,
            Listener(config.egress_listener_port, ConnectionType::EGRESS)
        );
        listeners.emplace(
            ConnectionType::INGRESS,
            Listener(config.ingress_listener_port, ConnectionType::INGRESS)
        );
    };