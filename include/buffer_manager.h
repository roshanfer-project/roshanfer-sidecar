#pragma once

#include "buffer.h"
#include "ring_helper.hpp"
#include <cstddef>
#include <memory>
#include <queue>
#include <sys/socket.h>

class BufferManager {

public:
  BufferManager(size_t, size_t);
  std::unique_ptr<Buffer> get_buffer();
  std::unique_ptr<Buffer> get_dn_buffer();
  void free_buffer(std::unique_ptr<Buffer>);
  void free_dn_buffer(std::unique_ptr<Buffer>);
  UserData *get_user_data();
  void free_user_data(UserData *&);

private:
  size_t count;
  size_t size;
  std::queue<std::unique_ptr<Buffer>> buffer_queue;
  std::queue<std::unique_ptr<Buffer>> dn_buffer_queue;
  std::queue<UserData *> user_data_queue;
};