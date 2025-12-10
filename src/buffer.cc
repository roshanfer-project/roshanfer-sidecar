#include "buffer.h"
#include "glog/logging.h"
#include <bits/types/struct_iovec.h>
#include <cstddef>
#include <cstring>
#include <memory>
#include <netinet/in.h>

Buffer::Buffer(size_t length, size_t id)
    : data(std::vector<char>(length)), is_free(true), size(length - 1),
      filled(0), index(id), msg(nullptr), addr(nullptr), iov(nullptr) {}

Buffer::~Buffer() {
  LOG(FATAL) << "Buffer deconstructor (should not be called)";
}

void Buffer::clear() {
  msg.reset();
  addr.reset();
  iov.reset();
  filled = 0;
  is_free = true;
}

void Buffer::set_filled(size_t f) {
  if (f > size) {
    LOG(FATAL) << "Buffer overflow, filled: " << f << ", size: " << size;
  }
  filled = f;
}

void Buffer::prepare_recvmsg() {
  // Zero-initialize ancillary structs to avoid garbage fields
  iov = std::make_unique<struct iovec>();
  std::memset(iov.get(), 0, sizeof(struct iovec));
  iov->iov_base = data.data();
  iov->iov_len = get_size();

  addr = std::make_unique<struct sockaddr_in>();
  std::memset(addr.get(), 0, sizeof(struct sockaddr_in));

  msg = std::make_unique<struct msghdr>();
  std::memset(msg.get(), 0, sizeof(struct msghdr));
  msg->msg_name = addr.get();
  msg->msg_namelen = sizeof(struct sockaddr_in);
  msg->msg_iov = iov.get();
  msg->msg_iovlen = 1;

  // Ensure no control buffer is exposed to kernel
  msg->msg_control = nullptr;
  msg->msg_controllen = 0;
  msg->msg_flags = 0;
}

void Buffer::prepare_reply_sendmsg(const std::unique_ptr<Buffer> &old_buffer) {
  iov = std::make_unique<struct iovec>();
  std::memset(iov.get(), 0, sizeof(struct iovec));
  iov->iov_base = data.data();
  iov->iov_len = get_filled();

  addr.reset(old_buffer->addr.release());
  msg = std::make_unique<struct msghdr>();
  std::memset(msg.get(), 0, sizeof(struct msghdr));
  msg->msg_name = addr.get();
  msg->msg_namelen = sizeof(struct sockaddr_in);
  msg->msg_iov = iov.get();
  msg->msg_iovlen = 1;

  // Ensure no control buffer is exposed to kernel
  msg->msg_control = nullptr;
  msg->msg_controllen = 0;
  msg->msg_flags = 0;
}

void Buffer::prepare_reply_sendmsg(struct sockaddr_in servaddr) {
  iov = std::make_unique<struct iovec>();
  std::memset(iov.get(), 0, sizeof(struct iovec));
  iov->iov_base = data.data();
  iov->iov_len = get_filled();

  addr = std::make_unique<struct sockaddr_in>(servaddr);
  msg = std::make_unique<struct msghdr>();
  std::memset(msg.get(), 0, sizeof(struct msghdr));
  msg->msg_name = addr.get();
  msg->msg_namelen = sizeof(struct sockaddr_in);
  msg->msg_iov = iov.get();
  msg->msg_iovlen = 1;

  // Ensure no control buffer is exposed to kernel
  msg->msg_control = nullptr;
  msg->msg_controllen = 0;
  msg->msg_flags = 0;
}

void Buffer::prepare_req_sendmsg(struct sockaddr_in servaddr) {
  iov = std::make_unique<struct iovec>();
  std::memset(iov.get(), 0, sizeof(struct iovec));
  iov->iov_base = data.data();
  iov->iov_len = get_filled();

  addr = std::make_unique<struct sockaddr_in>(servaddr);

  msg = std::make_unique<struct msghdr>();
  std::memset(msg.get(), 0, sizeof(struct msghdr));
  msg->msg_name = addr.get();
  msg->msg_namelen = sizeof(struct sockaddr_in);
  msg->msg_iov = iov.get();
  msg->msg_iovlen = 1;

  // Ensure no control buffer is exposed to kernel
  msg->msg_control = nullptr;
  msg->msg_controllen = 0;
  msg->msg_flags = 0;
}