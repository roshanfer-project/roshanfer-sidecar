#pragma once

#include "buffer.h"
#include "ring_helper.hpp"
#include "ring_wrapper.h"
#include <cstddef>
#include <memory>
#include <queue>
#include <sys/socket.h>

class BufferManager {

public:
  BufferManager(size_t, size_t, RingWrapper &);
  std::unique_ptr<Buffer> get_buffer();
  std::unique_ptr<Buffer> get_buffer_by_index(size_t);
  std::unique_ptr<Buffer> get_dn_buffer();
  void free_buffer(std::unique_ptr<Buffer>);
  void free_dn_buffer(std::unique_ptr<Buffer>);
  UserData *get_user_data();
  void free_user_data(UserData *&);

private:
  size_t count;
  size_t size;
  RingWrapper &ring;
  std::vector<std::unique_ptr<Buffer>> buffer_vector;
  std::queue<size_t> free_buffer_queue;
  std::vector<std::unique_ptr<Buffer>> dn_buffer_vector;
  std::queue<size_t> free_dn_buffer_queue;
  std::queue<UserData *> user_data_queue;
};