#pragma once

// #include "listener.h"
#include "utils.h"
#include <cstddef>
#include <cstdint>
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

  struct msghdr *get_msg() { return &msg; }
  void prepare_sendmsg(struct sockaddr_in);
  void clear();
  struct sockaddr_in get_addr() { return addr; }
  void set_addr(struct sockaddr_in a) { addr = a; }

public:
  std::vector<char> data;
  bool is_free;
  bool is_provided;
  int64_t enter_queue_ts;
  const std::string *ret_service = nullptr;
  RPCID ret_id = -1;

private:
  size_t size;
  size_t filled;
  size_t index;
  // HTTPConnection* conn;
  // Listener* listener;
  struct msghdr msg;
  struct sockaddr_in addr;
  struct iovec iov;
};