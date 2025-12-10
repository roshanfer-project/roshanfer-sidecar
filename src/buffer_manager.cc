#include "ring_helper.hpp"
#include <buffer.h>
#include <buffer_manager.h>
#include <cassert>
#include <cstddef>
#include <glog/logging.h>
#include <listener.h>
#include <memory>
#include <netinet/in.h>

UserData::UserData(size_t id)
    : buffer(), in_ring(false), listener(), conn(), op(Operation::ACCEPT),
      index(id), udp_type(UDPType::REQUEST),
      // rpc_message(nullptr),
      accept_addr(nullptr) {}

std::unique_ptr<Buffer> UserData::get_buffer() {
  if (!buffer) {
    LOG(FATAL) << "Buffer is null"
               << ", index: " << index << ", op: " << operation_to_str(op)
               << ", udp_type: " << udp_type_to_str(udp_type)
               << ", conn: " << conn << ", listener: " << listener
               << ", in_ring: " << in_ring;
  }
  return std::move(buffer);
}

void UserData::set_buffer(std::unique_ptr<Buffer> buf) {
  if (!buf) {
    LOG(FATAL) << "Buffer cannot be null";
  }
  buffer = std::move(buf);
}

void UserData::clear() {
  if (buffer) {
    LOG(FATAL) << "Buffer is not null";
  }
  buffer.reset();
  listener.reset();
  conn.reset();
  // rpc_message.reset();
  op = Operation::CLEAR;
  udp_type = UDPType::CLEAR;
  in_ring = false;
  accept_addr.reset();
}

BufferManager::BufferManager(size_t len, size_t buffer_size)
    : count(len), size(buffer_size), buffer_queue(), dn_buffer_queue(),
      user_data_queue() {
  for (size_t i = 0; i < count; i++) {
    user_data_queue.push(new UserData(i));
    buffer_queue.push(std::make_unique<Buffer>(size, i));
    dn_buffer_queue.push(std::make_unique<Buffer>(40, i + count));
  }
};

std::unique_ptr<Buffer> BufferManager::get_buffer() {
  if (buffer_queue.empty()) {
    LOG(FATAL) << "No free buffer available";
    return nullptr;
  }
  auto buffer = std::move(buffer_queue.front());
  buffer_queue.pop();
  if (!buffer->is_free) {
    LOG(FATAL) << "Buffer is not free, index: " << buffer->get_index();
    return nullptr;
  }
  buffer->is_free = false;
  return buffer;
}

std::unique_ptr<Buffer> BufferManager::get_dn_buffer() {
  if (dn_buffer_queue.empty()) {
    LOG(FATAL) << "No free dn buffer available";
    return nullptr;
  }
  auto buffer = std::move(dn_buffer_queue.front());
  dn_buffer_queue.pop();
  if (!buffer->is_free) {
    LOG(FATAL) << "Buffer is not free, index: " << buffer->get_index();
    return nullptr;
  }
  buffer->is_free = false;
  return buffer;
}

void BufferManager::free_dn_buffer(std::unique_ptr<Buffer> buffer) {
  if (!buffer) {
    LOG(FATAL) << "buffer cannot be null";
    return;
  }

  if (buffer->is_free) {
    LOG(FATAL) << "Buffer is already free, index: " << buffer->get_index();
    return;
  }
  buffer->clear();
  dn_buffer_queue.push(std::move(buffer));
}

void BufferManager::free_buffer(std::unique_ptr<Buffer> buffer) {
  if (!buffer) {
    LOG(FATAL) << "buffer cannot be null";
    return;
  }

  if (buffer->is_free) {
    LOG(FATAL) << "Buffer is already free, index: " << buffer->get_index();
    return;
  }
  buffer->clear();
  buffer_queue.push(std::move(buffer));
}

UserData *BufferManager::get_user_data() {
  if (user_data_queue.empty()) {
    LOG(FATAL) << "No free user data available";
  }
  UserData *ud = user_data_queue.front();
  user_data_queue.pop();
  VLOG(2) << "get new ud " << ud->index;
  return ud;
}

void BufferManager::free_user_data(UserData *&ud) {
  if (ud == nullptr) {
    LOG(FATAL) << "UserData cannot be null";
    return;
  }
  if (ud->in_ring) {
    LOG(FATAL) << "UserData is still in the ring, cannot free it";
  }
  ud->clear();
  user_data_queue.push(ud);
  VLOG(2) << "free ud " << ud->index;
  ud = nullptr;
}