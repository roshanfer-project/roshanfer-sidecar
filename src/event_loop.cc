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
    DLOG(INFO) << "Add initial accept submissions for all listeners";
    for (auto &listener : ingress_listeners.get_listeners()) {
        ring.prepare_accept(*listener.second.get(), buffer_manager.get_user_data());
    }

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
                TCPConnection& conn = ingress_listeners.add_connection(
                    cqe->res,
                    listener->get_port()
                );

                DLOG(INFO) << "Accepted connection on fd: " << listener->get_fd();

                // prepare read
                ring.prepare_read(
                    buffer_manager.get_buffer(std::addressof(conn), listener),
                    conn.get_fd(),
                    buffer_manager.get_user_data()
                );

                // re-arm accept
                ring.prepare_accept(*listener, buffer_manager.get_user_data());
                break;
            }
            
            case READ: {
                // read the data
                Buffer* buffer = reinterpret_cast<Buffer*>(ud->data);

                DLOG(INFO) << "Read completion event, fd: " << buffer->conn->get_fd();

                if (cqe->res < 0) {
                    LOG(FATAL) << "Failed to read from fd: " << buffer->conn->get_fd();
                } else if (cqe->res == 0) {
                    LOG(INFO) << "Connection closed on fd: " << buffer->conn->get_fd();
                    // free buffer
                    buffer_manager.free_buffer(buffer->index);
                    // remove connection
                    buffer->listener->remove_connection(buffer->conn->get_fd());
                    break;
                }

                DLOG(INFO) << "Read " << cqe->res << " bytes from buffer: " << buffer->data;
                buffer->filled = cqe->res;

                // free buffer
                //buffer_manager.free_buffer(buffer->index);

                // simple routing (known and fixed destination)
                // TODO: change routing
                try {
                    int dst_fd = state.route();
                    DLOG(INFO) << "Routing to fd: " << dst_fd;
                    
                    // prepare write
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

                // re-arm read
                ring.prepare_read(
                    buffer_manager.get_buffer(buffer->conn, buffer->listener),
                    buffer->conn->get_fd(),
                    buffer_manager.get_user_data()
                );
                break;
            }

            case CONNECT: {
                DLOG(INFO) << "Connect completion event";
                
                // update state machine
                TCPConnection* conn = reinterpret_cast<TCPConnection*>(ud->data);

                // get all buffers in the queue and write them to the connection
                // This assumes no blocking of messages
                // TODO: Ideally we want to route any individual buffer separately
                int dst_fd = state.route();
                std::unique_ptr<TCPConnection>& dst_conn = state.get_connection(dst_fd);
                DLOG(INFO) << "Routing to fd: " << dst_fd;
                while (state.has_buffer()) {
                    // update connection in buffer
                    Buffer* buffer = state.get_buffer();
                    buffer->conn = dst_conn.get();

                    // prepare write
                    ring.prepare_write(
                        dst_fd,
                        buffer,
                        buffer_manager.get_user_data()
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
    ingress_listeners(),
    state(config) {

    // Add listeners
    ingress_listeners.add_listener(config.ingress_port);
};