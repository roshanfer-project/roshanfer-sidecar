#include "listener.h"
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
    DLOG(INFO) << "Add initial accept submissions for egress listener";
    Buffer* buffer = buffer_manager.get_buffer();
    ring.prepare_accept(egress_listener, buffer_manager.get_user_data());

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

                DLOG(INFO) << "Accepted connection on fd: " << listener->get_fd();
                DLOG(INFO) << "New connection on fd: " << conn.get_fd();

                // get buffer and prepare it
                Buffer* buffer = buffer_manager.get_buffer();
                buffer->prepare_read(std::addressof(conn), listener);

                // prepare ring for read on the newly accepted connection
                ring.prepare_read(
                    buffer,
                    conn.get_fd(),
                    buffer_manager.get_user_data(),
                    ReqRes::REQUEST
                );

                // re-arm accept on the listener
                ring.prepare_accept(*listener, buffer_manager.get_user_data());
                break;
            }
            
            case READ: {
                // get the buffer from the user data
                Buffer* buffer = reinterpret_cast<Buffer*>(ud->data);

                // get the original connection
                auto& orig_conn = buffer->get_conn();
                auto& orig_listener = buffer->get_listener();
                DLOG(INFO) << "Read completion event, fd: " << orig_conn.get_fd();

                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to read from fd: " << orig_conn.get_fd();
                } else if (cqe->res == 0) {
                    LOG(INFO) << "Connection closed on fd: " << orig_conn.get_fd();

                    // free buffer
                    buffer_manager.free_buffer(buffer);

                    // remove connection from the appropriate object
                    if (ud->req_res == ReqRes::REQUEST) {
                        // remove connection from egress listener
                        orig_listener.remove_connection(orig_conn.get_fd());
                    } else if (ud->req_res == ReqRes::RESPONSE) {
                        // remove connection from connection pool
                        state.remove_connection(orig_conn.get_fd());
                    }
                    break;
                }

                DLOG(INFO) << "Read " << cqe->res << " bytes from buffer: " << buffer->data.get();
                buffer->set_filled(cqe->res);

                if (ud->req_res == ReqRes::REQUEST) {
                    DLOG(INFO) << "Request received";

                    DLOG(INFO) << "starting to parse";
                    orig_conn.parse(std::span<const char>(buffer->data.get(), buffer->get_filled()));

                    // simple routing (known and fixed destination)
                    // TODO: change routing
                    try {
                        int dst_fd = state.route(ConnectionType::EGRESS);
                        DLOG(INFO) << "Routing to fd: " << dst_fd;
                        
                        // prepare buffer for write
                        buffer->prepare_write(state.get_connection(dst_fd).get());

                        // prepare write (to write the request)
                        ring.prepare_write(
                            dst_fd,
                            buffer,
                            buffer_manager.get_user_data(),
                            ReqRes::REQUEST
                        );

                    } catch (AddConnectionException& e) {
                        // prepare connect
                        ring.prepare_connect(
                            e.conn,
                            buffer_manager.get_user_data()
                        );

                        // pass buffer to the state
                        state.add_buffer(buffer);
                    }
                } else if (ud->req_res == ReqRes::RESPONSE) {
                    DLOG(INFO) << "Response received";

                    // just return the response to THE one egress connection
                    // TODO: if we have multiple egress connections, we need to route the response
                    // to the correct connection
                    
                    // check if the local host is still connected
                    if (egress_listener.no_connections()) {
                        LOG(WARNING) << "No egress connection";
                    } else {
                        HTTPConnection* egress_conn = egress_listener.get_connections().begin()->second.get();
                        buffer->prepare_write(egress_conn);

                        // prepare write (to write the response)
                        ring.prepare_write(
                            egress_conn->get_fd(),
                            buffer,
                            buffer_manager.get_user_data(),
                            ReqRes::RESPONSE
                        );
                    }
                }
                

                // get a new buffer for read and prepare it
                buffer = buffer_manager.get_buffer();
                buffer->prepare_read(std::addressof(orig_conn), std::addressof(orig_listener));

                // re-arm read
                ring.prepare_read(
                    buffer,
                    orig_conn.get_fd(),
                    buffer_manager.get_user_data(),
                    ud->req_res
                );
                break;
            }

            case CONNECT: {
                HTTPConnection* orig_conn = reinterpret_cast<HTTPConnection*>(ud->data);

                DLOG(INFO) << "Connect completion event, fd: " << orig_conn->get_fd();

                // get all buffers in the queue and write them to the connection
                // This assumes no blocking of messages
                // TODO: Ideally we want to route any individual buffer separately
                int dst_fd = state.route(ConnectionType::EGRESS);
                std::unique_ptr<HTTPConnection>& dst_conn = state.get_connection(dst_fd);
                DLOG(INFO) << "Routing to fd: " << dst_fd;
                while (state.has_buffer()) {
                    // update connection in buffer
                    Buffer* buffer = state.get_buffer();
                    
                    // preprare buffer for the write
                    buffer->prepare_write(dst_conn.get());

                    // prepare write (to write the request)
                    ring.prepare_write(
                        dst_fd,
                        buffer,
                        buffer_manager.get_user_data(),
                        ReqRes::REQUEST
                    );

                    // arm the first read for this newly connected connection
                    Buffer* read_buffer = buffer_manager.get_buffer();
                    // for RESPONSE reads we don't need the listener in the case
                    // that we need to close the connection (we do it from connection pool)
                    read_buffer->prepare_read(orig_conn, nullptr);

                    // prepare read
                    ring.prepare_read(
                        read_buffer,
                        orig_conn->get_fd(),
                        buffer_manager.get_user_data(),
                        ReqRes::RESPONSE
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
    egress_listener(config.egress_listener_port, ConnectionType::EGRESS),
    //ingress_listener(config.ingress_port, ConnectionType::INGRESS),
    state(config) {};