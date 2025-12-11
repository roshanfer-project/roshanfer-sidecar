#include "buffer.h"
#include "glog/logging.h"
#include <bits/types/struct_iovec.h>
#include <cstddef>
#include <cstring>
#include <memory>
#include <netinet/in.h>

Buffer::Buffer(size_t length, size_t id)
    : data(std::vector<char>(length)), is_free(true), is_provided(false),
      size(length - 1), filled(0), index(id), msg({}), addr({}), iov({}) {}

Buffer::~Buffer() {
  LOG(FATAL) << "Buffer deconstructor (should not be called)";
}

void Buffer::clear() {
  std::memset(&msg, 0, sizeof(struct msghdr));
  std::memset(&addr, 0, sizeof(struct sockaddr_in));
  std::memset(&iov, 0, sizeof(struct iovec));
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
  iov.iov_base = data.data();
  iov.iov_len = get_size();

  msg.msg_name = &addr;
  msg.msg_namelen = sizeof(struct sockaddr_in);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;

  // Ensure no control buffer is exposed to kernel
  msg.msg_control = nullptr;
  msg.msg_controllen = 0;
  msg.msg_flags = 0;
}

void Buffer::prepare_reply_sendmsg(const std::unique_ptr<Buffer> &old_buffer) {
  iov.iov_base = data.data();
  iov.iov_len = get_filled();

  addr = old_buffer->addr;
  msg.msg_name = &addr;
  msg.msg_namelen = sizeof(struct sockaddr_in);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;

  // Ensure no control buffer is exposed to kernel
  msg.msg_control = nullptr;
  msg.msg_controllen = 0;
  msg.msg_flags = 0;
}

void Buffer::prepare_reply_sendmsg(struct sockaddr_in servaddr) {
  iov.iov_base = data.data();
  iov.iov_len = get_filled();

  addr = servaddr;
  msg.msg_name = &addr;
  msg.msg_namelen = sizeof(struct sockaddr_in);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;

  // Ensure no control buffer is exposed to kernel
  msg.msg_control = nullptr;
  msg.msg_controllen = 0;
  msg.msg_flags = 0;
}

void Buffer::prepare_req_sendmsg(struct sockaddr_in servaddr) {
  iov.iov_base = data.data();
  iov.iov_len = get_filled();

  addr = servaddr;
  msg.msg_name = &addr;
  msg.msg_namelen = sizeof(struct sockaddr_in);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;

  // Ensure no control buffer is exposed to kernel
  msg.msg_control = nullptr;
  msg.msg_controllen = 0;
  msg.msg_flags = 0;
}