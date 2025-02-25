#include <ring_wrapper.h>
#include <liburing.h>
#include <connection.h>
#include <listener.h>
#include <netinet/in.h>
#include <memory>
#include <buffer_manager.h>
#include "glog/logging.h"

RingWrapper::RingWrapper(int size)
: size(size) {
    // Initialize the ring
    // TODO: we should use the IORING_SETUP_SINGLE_ISSUER flag
    // which tells the kernel that only one thread will submit SQEs
    // the installed iouring (2.1) does not support it.
    int ret = io_uring_queue_init(size, &ring, 0);
    if (ret < 0) {
        throw std::runtime_error("Failed to initialize ring");
    }
    DLOG(INFO) << "Ring initialized";
}

RingWrapper::~RingWrapper() {
    io_uring_queue_exit(&ring);
}

void RingWrapper::prepare_accept(Listener& listener) {
    struct io_uring_sqe *sqe;
    sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        LOG(INFO) << "ring is full. Submitting...";
        DLOG(INFO) << "submited number: " << io_uring_submit(&ring);
        sqe = io_uring_get_sqe(&ring);
    }
    if (!sqe) {
        LOG(FATAL) << "Failed to get SQE";
    }
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listener.get_port());
    addr.sin_addr.s_addr = INADDR_ANY;
    socklen_t server_addr_len = sizeof(addr);

    io_uring_prep_bind(
        sqe,
        listener.get_fd(),
        reinterpret_cast<sockaddr*>(&addr),
        server_addr_len
    );

    io_uring_prep_listen(
        sqe,
        listener.get_fd(),
        10
    );

    io_uring_prep_accept(
        sqe,
        listener.get_fd(),
        reinterpret_cast<sockaddr*>(&addr),
        &server_addr_len, 
        0);
    
    struct UserData ud;
    ud.data = static_cast<void*>(&listener);
    ud.op = Operation::ACCEPT;
    io_uring_sqe_set_data(sqe, static_cast<void*>(&ud));
}

void RingWrapper::prepare_read(const std::unique_ptr<Buffer>& buffer, int fd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        throw std::runtime_error("Failed to get SQE");
    }

    UserData ud;
    ud.data = static_cast<void*>(buffer.get());
    ud.op = Operation::READ;

    // Set the buffer as the data for the SQE.
    // This will help us identify connection and buffer index after completion
    io_uring_sqe_set_data(sqe, static_cast<void*>(&ud));

    io_uring_prep_read(
        sqe,
        fd,
        buffer.get()->data.get(),
        buffer.get()->get_size(),
        0);
}

void RingWrapper::submit_and_wait() {
    int ret = io_uring_submit_and_wait(&ring, 1);
    if (ret < 0) {
        throw std::runtime_error("Failed to submit SQE");
    }
};

struct io_uring_cqe* RingWrapper::peek_cqe() {
    struct io_uring_cqe *cqe;
    int ret = io_uring_peek_cqe(&ring, &cqe);
    if (ret < 0) {
        throw std::runtime_error("Failed to peek CQE");
    }
    return cqe;
}

void RingWrapper::seen_cqe(struct io_uring_cqe* cqe) {
    io_uring_cqe_seen(&ring, cqe);
}

UserData* RingWrapper::get_user_data(struct io_uring_cqe* cqe) {
    return static_cast<UserData*>(io_uring_cqe_get_data(cqe));
}