#pragma once

#include "ring_helper.hpp"
#include <connection.hpp>
#include <cstddef>
#include <liburing.h>
#include <listener.hpp>
#include <memory>

class RingWrapper {
public:
  RingWrapper(size_t);
  ~RingWrapper();

  /**
   * @brief Submits an SQE for accepting connections to the ring.
   * @note It does not submit the SQE
   * @throws std::runtime_error if the submission fails
   * @todo make it multi-shot
   */
  void prepare_accept(std::shared_ptr<Listener>, UserData *);

  /**
   * @brief Submits an SQE for reading data from the ring
   * @throws std::runtime_error if the submission fails
   * @todo make it multi-shot
   */
  void prepare_read(UserData *, std::shared_ptr<Listener>,
                    std::shared_ptr<HTTPConnection>);

  void submit_and_wait();
  void submit();

  struct io_uring_cqe *peek_cqe();
  void seen_cqe(struct io_uring_cqe *);
  UserData *get_user_data(struct io_uring_cqe *);

  void prepare_connect(std::shared_ptr<HTTPConnection>, UserData *);
  void prepare_write(std::shared_ptr<HTTPConnection>, std::unique_ptr<Buffer>,
                     UserData *);
  void prepare_cancel(std::shared_ptr<HTTPConnection>, UserData *);

  void prepare_rcvmsg(int, UserData *);
  void prepare_sendmsg(int, std::unique_ptr<Buffer>, UserData *);
  void prepare_sendmsg_with_serveraddr(int, std::unique_ptr<Buffer>, UserData *,
                                       struct sockaddr_in);
  void add_buffer_to_ring(std::unique_ptr<Buffer> &, int);
  static void handle_multishot_recv(std::unique_ptr<Buffer> &buffer,
                                    int cqe_res);

private:
  struct io_uring_sqe *get_sqe();
  void set_user_data(struct io_uring_sqe *, UserData *);

private:
  struct io_uring ring;
  struct io_uring_buf_ring *ring_buf;
  struct io_uring_buf_ring *dn_ring_buf;
  size_t size;
};