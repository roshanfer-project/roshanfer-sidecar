#include "ring_helper.hpp"
#include <buffer.h>
#include <buffer_manager.h>
#include <cassert>
#include <cstddef>
#include <glog/logging.h>
#include <listener.h>
#include <memory>
#include <netinet/in.h>
#include <sys/types.h>

UserData::UserData(size_t id)
    : buffer(), in_ring(false), listener(), conn(), op(Operation::ACCEPT),
      index(id), udp_type(UDPType::REQUEST) {
  std::memset(&accept_addr, 0, sizeof(accept_addr));

  msg.msg_name = 0;
  msg.msg_namelen = sizeof(struct sockaddr_in);
  msg.msg_iov = 0;
  msg.msg_iovlen = 0;
}

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
  op = Operation::CLEAR;
  udp_type = UDPType::CLEAR;
  in_ring = false;
  std::memset(&accept_addr, 0, sizeof(accept_addr));
}

BufferManager::BufferManager(size_t len, size_t buffer_size, RingWrapper &ring)
    : count(len), size(buffer_size), ring(ring), buffer_vector(),
      udp_buffer_vector(), user_data_queue() {
  for (size_t i = 0; i < count; i++) {
    user_data_queue.push(new UserData(i));
    buffer_vector.push_back(std::make_unique<Buffer>(size, i));
    udp_buffer_vector.push_back(std::make_unique<Buffer>(256, i + count));

    if ((double)i < 0.8 * (double)count) {
      ring.add_buffer_to_ring(buffer_vector.back(), 0);
      ring.add_buffer_to_ring(udp_buffer_vector.back(), 1);
      buffer_vector.back()->is_provided = true;
      udp_buffer_vector.back()->is_provided = true;
    } else {
      free_buffer_queue.push(i);
      free_udp_buffer_queue.push(i + count);
    }
  }
};

std::unique_ptr<Buffer> BufferManager::get_buffer() {
  if (free_buffer_queue.empty()) {
    LOG(FATAL) << "No free buffer available";
    return nullptr;
  }
  auto buffer = std::move(buffer_vector.at(free_buffer_queue.front()));
  free_buffer_queue.pop();
  if (!buffer->is_free) {
    LOG(FATAL) << "Buffer is not free, index: " << buffer->get_index();
    return nullptr;
  }
  buffer->is_free = false;
  return buffer;
}

std::unique_ptr<Buffer> BufferManager::get_buffer_by_index(size_t index) {
  if (index >= count) {
    LOG(FATAL) << "TCP buffer id out of range: " << index << " (count=" << count
               << ") — wrong buffer group on CQE?";
  }
  if (buffer_vector.at(index) == nullptr) {
    LOG(FATAL) << "Buffer is null, index: " << index;
    return nullptr;
  }
  auto buffer = std::move(buffer_vector.at(index));
  if (!buffer->is_free) {
    LOG(FATAL) << "Buffer is not free, index: " << buffer->get_index();
    return nullptr;
  }
  buffer->is_free = false;
  return buffer;
}

std::unique_ptr<Buffer> BufferManager::get_udp_buffer_by_index(size_t index) {
  if (index < count || index >= count + udp_buffer_vector.size()) {
    LOG(FATAL) << "UDP buffer id out of range: " << index << " (expect [" << count
               << ", " << count + udp_buffer_vector.size()
               << ")) — TCP id or corrupt CQE flags?";
  }
  if (udp_buffer_vector.at(index - count) == nullptr) {
    LOG(FATAL) << "Buffer is null, index: " << index;
    return nullptr;
  }
  auto buffer = std::move(udp_buffer_vector.at(index - count));
  if (!buffer->is_free) {
    LOG(FATAL) << "Buffer is not free, index: " << buffer->get_index();
    return nullptr;
  }
  buffer->is_free = false;
  return buffer;
}

std::unique_ptr<Buffer> BufferManager::get_udp_buffer() {
  if (free_udp_buffer_queue.empty()) {
    LOG(FATAL) << "No free UDP buffer available";
    return nullptr;
  }
  auto buffer =
      std::move(udp_buffer_vector.at(free_udp_buffer_queue.front() - count));
  free_udp_buffer_queue.pop();
  if (!buffer->is_free) {
    LOG(FATAL) << "Buffer is not free, index: " << buffer->get_index();
    return nullptr;
  }
  buffer->is_free = false;
  return buffer;
}

void BufferManager::free_udp_buffer(std::unique_ptr<Buffer> buffer) {
  if (!buffer) {
    LOG(FATAL) << "buffer cannot be null";
    return;
  }

  if (buffer->is_free) {
    LOG(FATAL) << "Buffer is already free, index: " << buffer->get_index();
    return;
  }
  buffer->clear();
  size_t index = buffer->get_index();
  bool is_provided = buffer->is_provided;
  udp_buffer_vector.at(index - count) = std::move(buffer);
  if (is_provided) {
    ring.add_buffer_to_ring(udp_buffer_vector.at(index - count),
                            1 // UDP buffer group
    );
  } else {
    free_udp_buffer_queue.push(index);
  }
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
  size_t index = buffer->get_index();
  bool is_provided = buffer->is_provided;
  buffer_vector.at(index) = std::move(buffer);
  if (is_provided) {
    ring.add_buffer_to_ring(buffer_vector.at(index),
                            0 // TCP buffer group
    );
  } else {
    free_buffer_queue.push(index);
  }
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