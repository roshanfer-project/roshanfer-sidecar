#pragma once

#include <liburing.h>
#include <connection.h>
#include <listener.h>
#include <memory>

enum Operation {
    ACCEPT,
    READ,
    WRITE,
    CONNECT
};

struct UserData {
    void* data;
    enum Operation op;
};

class RingWrapper {
public:
    RingWrapper(int);
    ~RingWrapper();

    /**
     * @brief Submits an SQE for accepting connections to the ring.
     * @note It does not submit the SQE
     * @throws std::runtime_error if the submission fails
     * @todo make it multi-shot
     */
    void prepare_accept(Listener&);

    /**
     * @brief Submits an SQE for reading data from the ring
     * @throws std::runtime_error if the submission fails
     * @todo make it multi-shot
     */
    void prepare_read(const std::unique_ptr<Buffer>&, int);

    void submit_and_wait();

    struct io_uring_cqe* peek_cqe();
    void seen_cqe(struct io_uring_cqe*);
    UserData* get_user_data(struct io_uring_cqe*);

private:
    struct io_uring ring;
    int size;
};