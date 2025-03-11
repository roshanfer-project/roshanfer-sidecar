#pragma once

#include <liburing.h>
#include <listener.h>
#include <connection.h>
#include <buffer_manager.h>

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
    void prepare_accept(Listener&, UserData*);

    /**
     * @brief Submits an SQE for reading data from the ring
     * @throws std::runtime_error if the submission fails
     * @todo make it multi-shot
     */
    void prepare_read(Buffer*, int, UserData*, ReqRes req_res);

    void submit_and_wait();

    struct io_uring_cqe* peek_cqe();
    void seen_cqe(struct io_uring_cqe*);
    UserData* get_user_data(struct io_uring_cqe*);

    void prepare_connect(std::unique_ptr<HTTPConnection>&, UserData*);
    void prepare_write(int, Buffer*, UserData*, ReqRes);

private:
    struct io_uring_sqe* get_sqe();

private:
    struct io_uring ring;
    int size;
};