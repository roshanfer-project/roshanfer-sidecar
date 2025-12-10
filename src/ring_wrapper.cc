#include "buffer.h"
#include "glog/logging.h"
#include "ring_helper.hpp"
#include <buffer_manager.h>
#include <connection.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <liburing.h>
#include <listener.h>
#include <memory>
#include <netinet/in.h>
#include <ring_wrapper.h>
#include <unistd.h>

RingWrapper::RingWrapper(size_t ring_size) : size(ring_size) {
  // Initialize the ring
  uint32_t flags = IORING_SETUP_SINGLE_ISSUER;
  int ret = io_uring_queue_init((uint32_t)size, &ring, flags);
  if (ret < 0) {
    throw std::runtime_error("Failed to initialize ring: " +
                             std::string(strerror(-ret)));
  }
  VLOG(1) << "Ring initialized";
}

RingWrapper::~RingWrapper() { io_uring_queue_exit(&ring); }

void RingWrapper::prepare_accept(std::shared_ptr<Listener> listener,
                                 UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();
  int fd = listener->get_fd();

  ud->accept_addr = std::make_unique<struct sockaddr_in>();
  // zero initialize the structure
  std::memset(ud->accept_addr.get(), 0, sizeof(struct sockaddr_in));
  socklen_t server_addr_len = sizeof(*ud->accept_addr.get());

  io_uring_prep_accept(sqe, fd,
                       reinterpret_cast<sockaddr *>(ud->accept_addr.get()),
                       &server_addr_len, 0);

  ud->listener = std::move(listener);
  ud->op = Operation::ACCEPT;
  set_user_data(sqe, ud);
  VLOG(1) << "Prepared accept, fd: " << fd;
}

void RingWrapper::prepare_read(std::unique_ptr<Buffer> buffer, UserData *ud,
                               std::shared_ptr<Listener> listener,
                               std::shared_ptr<HTTPConnection> conn) {
  struct io_uring_sqe *sqe = get_sqe();
  int fd = conn->get_fd();

  io_uring_prep_read(sqe, fd, buffer->data.data(), (uint32_t)buffer->get_size(),
                     0);

  ::prepare_read(ud, std::move(buffer), std::move(listener), std::move(conn));

  // Set the buffer as the data for the SQE.
  // This will help us identify connection and buffer index after completion
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared read, fd: " << fd;
}

void RingWrapper::submit_and_wait() {
  unsigned sq_head = *ring.sq.khead;
  unsigned sq_tail = *ring.sq.ktail;
  unsigned cq_head = *ring.cq.khead;
  unsigned cq_tail = *ring.cq.ktail;

  if ((sq_tail - sq_head) > ring.sq.ring_entries) {
    LOG(FATAL) << "SQ overflow: tail=" << sq_tail << " head=" << sq_head
               << " entries=" << ring.sq.ring_entries;
  }
  if ((cq_tail - cq_head) > ring.cq.ring_entries) {
    LOG(FATAL) << "CQ overflow: tail=" << cq_tail << " head=" << cq_head
               << " entries=" << ring.cq.ring_entries;
  }

  int ret = io_uring_submit_and_wait(&ring, 1);
  if (ret < 0) {
    LOG(FATAL) << "Failed to submit and wait, ret: " << ret;
  }
};

void RingWrapper::submit() {
  int ret = io_uring_submit(&ring);
  if (ret < 0) {
    LOG(FATAL) << "Failed to submit, ret: " << ret;
  }
}

struct io_uring_cqe *RingWrapper::peek_cqe() {
  struct io_uring_cqe *cqe;
  int ret = io_uring_peek_cqe(&ring, &cqe);
  if (ret == -EAGAIN) {
    VLOG(1) << "No completion event.";
    seen_cqe(cqe);
    return nullptr;
  }
  if (ret < 0) {
    LOG(FATAL) << "Failed to peek CQE, ret: " << ret;
  }
  return cqe;
}

void RingWrapper::seen_cqe(struct io_uring_cqe *cqe) {
  io_uring_cqe_seen(&ring, cqe);
}

UserData *RingWrapper::get_user_data(struct io_uring_cqe *cqe) {
  UserData *ret = static_cast<UserData *>(io_uring_cqe_get_data(cqe));
  if (ret == nullptr) {
    LOG(FATAL) << "UserData is null, cqe: " << cqe;
  }
  if (ret->op == Operation::CLEAR) {
    LOG(FATAL) << "UserData is in CLEAR state, index: " << ret->index;
  }
  if (ret->in_ring == false) {
    LOG(FATAL) << "UserData is not in ring, this should not happen";
  }
  ret->in_ring = false;
  VLOG(2) << "get ud " << ret->index;
  return ret;
}

void RingWrapper::set_user_data(struct io_uring_sqe *sqe, UserData *ud) {
  if (ud == nullptr) {
    LOG(FATAL) << "UserData cannot be null";
  }
  if (ud->op == Operation::CLEAR) {
    LOG(FATAL) << "UserData is in CLEAR state, this should not happen";
  }
  if (ud->in_ring) {
    LOG(FATAL) << "UserData is already in ring, this should not happen";
  }
  ud->in_ring = true;
  io_uring_sqe_set_data(sqe, static_cast<void *>(ud));
  VLOG(2) << "set ud " << ud->index;
}

struct io_uring_sqe *RingWrapper::get_sqe() {
  struct io_uring_sqe *sqe;
  sqe = io_uring_get_sqe(&ring);
  if (!sqe) {
    LOG(INFO) << "ring is full. Submitting...";
    VLOG(1) << "submited number: " << io_uring_submit(&ring);
    sqe = io_uring_get_sqe(&ring);
  }
  if (!sqe) {
    LOG(FATAL) << "Failed to get SQE";
  }
  std::memset(sqe, 0, sizeof(*sqe));
  return sqe;
}

void RingWrapper::prepare_connect(std::shared_ptr<HTTPConnection> conn,
                                  UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();
  int fd = conn->get_fd();

  io_uring_prep_connect(sqe, fd, conn->get_addr(), sizeof(*conn->get_addr()));

  ud->conn = std::move(conn);
  ud->op = Operation::CONNECT;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared connect, fd: " << fd;
}

void RingWrapper::prepare_write(std::shared_ptr<HTTPConnection> conn,
                                std::unique_ptr<Buffer> buffer, UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();
  int fd = conn->get_fd();

  io_uring_prep_write(sqe, fd, buffer->data.data(),
                      (uint32_t)buffer->get_filled(), 0);

  ::prepare_write(ud, std::move(buffer), std::move(conn));
  set_user_data(sqe, ud);
  VLOG(1) << "Prepared write, fd: " << fd;
}

void RingWrapper::prepare_cancel(std::shared_ptr<HTTPConnection> conn,
                                 UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();
  int fd = conn->get_fd();

  io_uring_prep_close(sqe, fd);

  ud->conn = std::move(conn);
  ud->op = Operation::CANCEL;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared cancel, fd: " << fd;
}

void RingWrapper::prepare_rcvmsg(int fd, std::unique_ptr<Buffer> buffer,
                                 UserData *ud, UDPType udp_type) {
  struct io_uring_sqe *sqe = get_sqe();

  buffer->prepare_recvmsg();

  io_uring_prep_recvmsg(sqe, fd, buffer->get_msg().get(), 0);

  // io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);

  ud->set_buffer(std::move(buffer));
  ud->op = Operation::RCVMSG;
  ud->udp_type = udp_type;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared rcvmsg, fd: " << fd;
}

/*
    This a "reply" send message because it uses the server address structure
    from the received message (old_buffer). In the case of a "request" send
   message, the server address structure has to created manually.
*/
void RingWrapper::prepare_reply_sendmsg(
    int fd, const std::unique_ptr<Buffer> &old_buffer,
    std::unique_ptr<Buffer> new_buffer, UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();

  new_buffer->prepare_reply_sendmsg(old_buffer);

  io_uring_prep_sendmsg(sqe, fd, new_buffer->get_msg().get(), 0);

  // io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);

  ud->set_buffer(std::move(new_buffer));
  ud->op = Operation::SENDMSG;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared sendmsg (reply), fd: " << fd;
}

void RingWrapper::prepare_reply_sendmsg(int fd,
                                        std::unique_ptr<Buffer> new_buffer,
                                        UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();

  io_uring_prep_sendmsg(sqe, fd, new_buffer->get_msg().get(), 0);

  ud->set_buffer(std::move(new_buffer));
  ud->op = Operation::SENDMSG;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared sendmsg (reply), fd: " << fd;
}

void RingWrapper::prepare_req_sendmsg(int fd, std::unique_ptr<Buffer> buffer,
                                      UserData *ud,
                                      struct sockaddr_in servaddr) {
  struct io_uring_sqe *sqe = get_sqe();

  buffer->prepare_req_sendmsg(servaddr);

  io_uring_prep_sendmsg(sqe, fd, buffer->get_msg().get(), 0);

  ud->set_buffer(std::move(buffer));
  ud->op = Operation::SENDMSG;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared sendmsg (req), fd: " << fd;
}