#include "buffer.hpp"
#include "glog/logging.h"
#include <bits/types/struct_iovec.h>
#include <cstddef>
#include <cstring>
#include <netinet/in.h>

Buffer::Buffer(size_t length, size_t id)
    : data(std::vector<char>(length)),  size(length - 1),  index(id), msg({}),
      addr({}), iov({}) {}

Buffer::~Buffer() {
  LOG(FATAL) << "Buffer deconstructor (should not be called)";
}

void Buffer::clear() {
  std::memset(&msg, 0, sizeof(struct msghdr));
  std::memset(&addr, 0, sizeof(struct sockaddr_in));
  std::memset(&iov, 0, sizeof(struct iovec));
  filled = 0;
  is_free = true;
  enter_queue_ts = 0;
}

void Buffer::set_filled(size_t f) {
  if (f > size) {
    LOG(FATAL) << "Buffer overflow, filled: " << f << ", size: " << size;
  }
  filled = f;
}

/*
  functionalities: 1. set the destination address 2. set the message content
*/
void Buffer::prepare_sendmsg(struct sockaddr_in servaddr) {
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