#include "buffer.h"
#include "config.h"
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
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

RingWrapper::RingWrapper(size_t ring_size) : size(ring_size) {
  // Initialize the ring
  uint32_t flags = IORING_SETUP_SINGLE_ISSUER;
  if (config.is_ingress) {
    flags |= IORING_SETUP_SQPOLL;
  }
  int ret = io_uring_queue_init((uint32_t)size, &ring, flags);
  if (ret < 0) {
    throw std::runtime_error("Failed to initialize ring: " +
                             std::string(strerror(-ret)));
  }
  VLOG(1) << "Ring initialized";

  // Initialize the buffer ring
  ssize_t page_size = sysconf(_SC_PAGESIZE);
  if (page_size < 0) {
    LOG(FATAL) << "Failed to get page size"
               << ", error: " << strerror(errno);
  }
  LOG(INFO) << "Page size: " << page_size;

  /* check if buffer count is power of 2 */
  if (config.buffer_count & (config.buffer_count - 1)) {
    LOG(FATAL) << "Buffer count is not power of 2";
  }

  if (config.buffer_count > 32 * 1024 * 1024) {
    LOG(FATAL) << "Buffer count is too large";
  }

  /* allocate mem for sharing buffer ring */
  if (posix_memalign((void **)&ring_buf, (size_t)page_size,
                     config.buffer_count * sizeof(struct io_uring_buf_ring)) !=
      0) {
    LOG(FATAL) << "Failed to allocate memory for buffer ring"
               << ", error: " << strerror(errno);
  }

  /* allocate mem for sharing buffer ring */
  if (posix_memalign((void **)&dn_ring_buf, (size_t)page_size,
                     config.buffer_count * sizeof(struct io_uring_buf_ring)) !=
      0) {
    LOG(FATAL) << "Failed to allocate memory for dn buffer ring"
               << ", error: " << strerror(errno);
  }

  /* assign and register buffer ring */
  struct io_uring_buf_reg reg;
  std::memset(&reg, 0, sizeof(reg));
  reg.ring_addr = (unsigned long)ring_buf;
  reg.ring_entries = (uint32_t)config.buffer_count;
  reg.bgid = 0;
  if (io_uring_register_buf_ring(&ring, &reg, 0) != 0) {
    LOG(FATAL) << "Failed to register buffer ring"
               << ", error: " << strerror(errno);
  }
  io_uring_buf_ring_init(ring_buf);

  /* assign and register dn buffer ring */
  struct io_uring_buf_reg dn_reg;
  std::memset(&dn_reg, 0, sizeof(dn_reg));
  dn_reg.ring_addr = (unsigned long)dn_ring_buf;
  dn_reg.ring_entries = (uint32_t)config.buffer_count;
  dn_reg.bgid = 1;
  if (io_uring_register_buf_ring(&ring, &dn_reg, 0) != 0) {
    LOG(FATAL) << "Failed to register dn buffer ring"
               << ", error: " << strerror(errno);
  }
  io_uring_buf_ring_init(dn_ring_buf);
}

RingWrapper::~RingWrapper() { io_uring_queue_exit(&ring); }

void RingWrapper::add_buffer_to_ring(std::unique_ptr<Buffer> &buffer,
                                     int bgid) {
  struct io_uring_buf_ring *buf = bgid == 0 ? ring_buf : dn_ring_buf;
  io_uring_buf_ring_add(
      buf, static_cast<void *>(buffer->data.data()),
      (uint32_t)buffer->get_size(), (uint16_t)buffer->get_index(),
      io_uring_buf_ring_mask((uint32_t)config.buffer_count), 0);

  io_uring_buf_ring_advance(buf, 1);
}

void RingWrapper::prepare_accept(std::shared_ptr<Listener> listener,
                                 UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();
  int fd = listener->get_fd();

  // zero initialize the structure
  std::memset(&ud->accept_addr, 0, sizeof(struct sockaddr_in));
  socklen_t server_addr_len = sizeof(ud->accept_addr);

  io_uring_prep_accept(sqe, fd, reinterpret_cast<sockaddr *>(&ud->accept_addr),
                       &server_addr_len, 0);

  ud->listener = std::move(listener);
  ud->op = Operation::ACCEPT;
  set_user_data(sqe, ud);
  VLOG(1) << "Prepared accept, fd: " << fd;
}

void RingWrapper::prepare_read(UserData *ud, std::shared_ptr<Listener> listener,
                               std::shared_ptr<HTTPConnection> conn) {
  struct io_uring_sqe *sqe = get_sqe();
  sqe->buf_group = 0; // TCP buffer group
  int fd = conn->get_fd();

  io_uring_prep_recv_multishot(sqe, fd, NULL, 0, 0);
  io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);

  ::prepare_read(ud, std::move(listener), std::move(conn));

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

  io_uring_prep_connect(sqe, fd, conn->get_addr(), sizeof(struct sockaddr));

  ud->conn = std::move(conn);
  ud->op = Operation::CONNECT;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared connect, fd: " << fd;
}

void RingWrapper::prepare_write(std::shared_ptr<HTTPConnection> conn,
                                std::unique_ptr<Buffer> buffer, UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();
  int fd = conn->get_fd();

  io_uring_prep_send(sqe, fd, buffer->data.data(),
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

void RingWrapper::prepare_rcvmsg(int fd, UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();
  io_uring_prep_recvmsg_multishot(sqe, fd, &ud->msg, 0);
  sqe->buf_group = 1; // UDP buffer group
  io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);

  ud->op = Operation::RCVMSG;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared rcvmsg, fd: " << fd;
}

void RingWrapper::prepare_sendmsg(int fd, std::unique_ptr<Buffer> new_buffer,
                                  UserData *ud) {
  struct io_uring_sqe *sqe = get_sqe();

  io_uring_prep_sendmsg(sqe, fd, new_buffer->get_msg(), 0);

  ud->set_buffer(std::move(new_buffer));
  ud->op = Operation::SENDMSG;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared sendmsg (reply), fd: " << fd;
}

void RingWrapper::prepare_sendmsg_with_serveraddr(
    int fd, std::unique_ptr<Buffer> buffer, UserData *ud,
    struct sockaddr_in servaddr) {
  struct io_uring_sqe *sqe = get_sqe();

  buffer->prepare_sendmsg(servaddr);

  io_uring_prep_sendmsg(sqe, fd, buffer->get_msg(), 0);

  ud->set_buffer(std::move(buffer));
  ud->op = Operation::SENDMSG;
  set_user_data(sqe, ud);

  VLOG(1) << "Prepared sendmsg (req), fd: " << fd;
}

void RingWrapper::handle_multishot_recv(std::unique_ptr<Buffer> &buffer,
                                        int cqe_res) {
  // Handle multishot header
  struct io_uring_recvmsg_out *out =
      reinterpret_cast<struct io_uring_recvmsg_out *>(buffer->data.data());

  size_t header_len = sizeof(struct io_uring_recvmsg_out);
  if (out->namelen > 0) {
    struct sockaddr *addr =
        reinterpret_cast<struct sockaddr *>(buffer->data.data() + header_len);
    if (out->namelen == sizeof(struct sockaddr_in)) {
      buffer->set_addr(*reinterpret_cast<struct sockaddr_in *>(addr));
    } else {
      LOG(FATAL) << "Unexpected address length: " << out->namelen;
    }
    header_len += out->namelen;
  }

  header_len += out->controllen;

  if ((size_t)cqe_res < header_len) {
    LOG(FATAL) << "Received message smaller than header length";
  }

  size_t payload_len = (size_t)cqe_res - header_len;

  // Move payload to start of buffer
  if (payload_len > 0) {
    std::memmove(buffer->data.data(), buffer->data.data() + header_len,
                 payload_len);
  }

  buffer->set_filled(payload_len);
}