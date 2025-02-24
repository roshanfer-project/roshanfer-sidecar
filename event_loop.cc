#include <event_loop.h>
#include <connection.h>
#include <buffer_manager.h>
#include <ring_wrapper.h>
#include <iostream>

void EventLoop::run() {
    // Pointers to accept and identify completion events
    struct io_uring_cqe *cqe;
    struct UserData *ud;

    // Add accept submissions
    ingress_listeners.listen_all(ring);

    // main event loop
    while(true) {
        ring.submit_and_wait();

        while((cqe = ring.peek_cqe())) {
            // Identify the type of event
            ud = ring.get_user_data(cqe);

            // Handle the event
            switch (ud->op)
            {
            case ACCEPT:
                // create connection
                Listener& listener = reinterpret_cast<Listener&>(ud->data);
                TCPConnection& conn = ingress_listeners.add_connection(
                    cqe->res,
                    listener.get_port()
                );
                // prepare read
                ring.prepare_read(buffer_manager.get_buffer(std::addressof(listener)),
                        conn.get_fd());

                // re-arm accept
                ring.prepare_accept(listener);
                break;
            
            case READ:
                // read the data
                Buffer& buffer = reinterpret_cast<Buffer&>(ud->data);
                // TODO: use a propper logger
                std::cout << "Read from buffer" << buffer.data << std::endl;

                // free buffer
                buffer_manager.free_buffer(buffer.index);

                // TODO: prepare write

                // re-arm read
                ring.prepare_read(
                    buffer_manager.get_buffer(buffer.listener),
                    buffer.listener->get_fd()
                );
                break;
            
            default:
                break;
            }

            // Advance the ring
            ring.seen_cqe(cqe);

        }
    }
};

EventLoop::EventLoop(Config config)
:   ring(config.ring_size),
    buffer_manager(config.buffer_count, config.buffer_size),
    ingress_listeners() {

    // Add listeners
    ingress_listeners.add_listener(config.listen_port);
};