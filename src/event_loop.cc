#include "listener.h"
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
                // create connection
                Listener* listener = reinterpret_cast<Listener*>(ud->data);
                TCPConnection& conn = listener->add_connection(cqe->res);

                DLOG(INFO) << "Accepted connection on fd: " << listener->get_fd();
                DLOG(INFO) << "New connection on fd: " << conn.get_fd();

                // prepare read
                ring.prepare_read(
                    buffer_manager.get_buffer(std::addressof(conn), listener),
                    conn.get_fd(),
                    buffer_manager.get_user_data(),
                    ReqRes::REQUEST
                );

                // re-arm accept
                ring.prepare_accept(*listener, buffer_manager.get_user_data());
                break;
            }
            
            case READ: {
                // read the data
                Buffer* buffer = reinterpret_cast<Buffer*>(ud->data);
                
                auto* orig_conn = buffer->conn;
                DLOG(INFO) << "Read completion event, fd: " << orig_conn->get_fd();

                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to read from fd: " << buffer->conn->get_fd();
                } else if (cqe->res == 0) {
                    LOG(INFO) << "Connection closed on fd: " << buffer->conn->get_fd();
                    // free buffer
                    buffer_manager.free_buffer(buffer->index);
                    if (ud->req_res == ReqRes::REQUEST) {
                        // remove connection from egress listener
                        buffer->listener->remove_connection(buffer->conn->get_fd());
                    } else if (ud->req_res == ReqRes::RESPONSE) {
                        // remove connection
                        state.remove_connection(buffer->conn->get_fd());
                    }
                    break;
                }

                DLOG(INFO) << "Read " << cqe->res << " bytes from buffer: " << buffer->data;
                buffer->filled = cqe->res;

                if (ud->req_res == ReqRes::REQUEST) {
                    DLOG(INFO) << "Request received";

                    // simple routing (known and fixed destination)
                    // TODO: change routing
                    try {
                        int dst_fd = state.route(ConnectionType::EGRESS);
                        DLOG(INFO) << "Routing to fd: " << dst_fd;
                        
                        // prepare write
                        buffer->conn = state.get_connection(dst_fd).get();
                        ring.prepare_write(
                            dst_fd,
                            buffer,
                            buffer_manager.get_user_data()
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

                    buffer->conn = egress_listener.get_connections().begin()->second.get();
                    ring.prepare_write(
                        egress_listener.get_connections().begin()->second->get_fd(),
                        buffer,
                        buffer_manager.get_user_data()
                    );
                }
                

                // re-arm read
                ring.prepare_read(
                    buffer_manager.get_buffer(orig_conn, buffer->listener),
                    orig_conn->get_fd(),
                    buffer_manager.get_user_data(),
                    ud->req_res
                );
                break;
            }

            case CONNECT: {
                TCPConnection* conn = reinterpret_cast<TCPConnection*>(ud->data);

                DLOG(INFO) << "Connect completion event, fd: " << conn->get_fd();

                // get all buffers in the queue and write them to the connection
                // This assumes no blocking of messages
                // TODO: Ideally we want to route any individual buffer separately
                int dst_fd = state.route(ConnectionType::EGRESS);
                std::unique_ptr<TCPConnection>& dst_conn = state.get_connection(dst_fd);
                DLOG(INFO) << "Routing to fd: " << dst_fd;
                while (state.has_buffer()) {
                    // update connection in buffer
                    Buffer* buffer = state.get_buffer();
                    buffer->conn = dst_conn.get();

                    // prepare write (to write the request)
                    ring.prepare_write(
                        dst_fd,
                        buffer,
                        buffer_manager.get_user_data()
                    );

                    // prepare read (to read the response)
                    ring.prepare_read(
                        buffer_manager.get_buffer(dst_conn.get(), nullptr),
                        dst_fd,
                        buffer_manager.get_user_data(),
                        ReqRes::RESPONSE
                    );
                }
                break;
            }

            case WRITE: {
                DLOG(INFO) << "Write completion event";
                Buffer* buffer = reinterpret_cast<Buffer*>(ud->data);

                DLOG(INFO) << "Wrote " << cqe->res << " bytes to fd: " << buffer->conn->get_fd();

                // free buffer
                buffer_manager.free_buffer(buffer->index);
                break;
            }
            
            default:
                break;
            }

            // free the user data
            buffer_manager.free_user_data(ud->index);

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