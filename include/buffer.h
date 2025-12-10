#pragma once

// #include "listener.h"
#include <cstddef>
#include <memory>
#include <netinet/in.h>
#include <vector>

class Buffer {

public:
  Buffer(size_t, size_t);
  ~Buffer();

  // delete copy semantics
  Buffer(const Buffer &) = delete;
  Buffer &operator=(const Buffer &) = delete;

  // delete move semantics
  Buffer(Buffer &&) = delete;
  Buffer &operator=(Buffer &&) = delete;

  size_t get_size() { return size; }
  size_t get_filled() { return filled; }
  size_t get_index() { return index; }
  void set_filled(size_t f);

  void prepare_recvmsg();
  std::unique_ptr<struct msghdr> &get_msg() { return msg; }
  void prepare_reply_sendmsg(const std::unique_ptr<Buffer> &old_buffer);
  void prepare_reply_sendmsg(struct sockaddr_in);
  void prepare_req_sendmsg(struct sockaddr_in);
  void clear();
  struct sockaddr_in get_addr() { return *addr.get(); }

public:
  std::vector<char> data;
  bool is_free;

private:
  size_t size;
  size_t filled;
  size_t index;
  // HTTPConnection* conn;
  // Listener* listener;
  std::unique_ptr<struct msghdr> msg;
  std::unique_ptr<struct sockaddr_in> addr;
  std::unique_ptr<struct iovec> iov;
};