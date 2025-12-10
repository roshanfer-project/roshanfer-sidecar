#pragma once

#include <buffer_manager.h>
#include <connection.h>
#include <cstddef>
#include <liburing.h>
#include <listener.h>
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
  void prepare_read(std::unique_ptr<Buffer>, UserData *,
                    std::shared_ptr<Listener>, std::shared_ptr<HTTPConnection>);

  void submit_and_wait();
  void submit();

  struct io_uring_cqe *peek_cqe();
  void seen_cqe(struct io_uring_cqe *);
  UserData *get_user_data(struct io_uring_cqe *);

  void prepare_connect(std::shared_ptr<HTTPConnection>, UserData *);
  void prepare_write(std::shared_ptr<HTTPConnection>, std::unique_ptr<Buffer>,
                     UserData *);
  void prepare_cancel(std::shared_ptr<HTTPConnection>, UserData *);

  void prepare_rcvmsg(int, std::unique_ptr<Buffer>, UserData *, UDPType);
  void prepare_reply_sendmsg(int, const std::unique_ptr<Buffer> &,
                             std::unique_ptr<Buffer>, UserData *);
  void prepare_reply_sendmsg(int, std::unique_ptr<Buffer>, UserData *);
  void prepare_req_sendmsg(int, std::unique_ptr<Buffer>, UserData *,
                           struct sockaddr_in);

private:
  struct io_uring_sqe *get_sqe();
  void set_user_data(struct io_uring_sqe *, UserData *);

private:
  struct io_uring ring;
  size_t size;
};