#include <buffer.h>
#include "ring_helper.h"
#include <buffer_manager.h>
#include <cassert>
#include <cstddef>
#include <memory>
#include <netinet/in.h>
#include <listener.h>
#include <glog/logging.h>

UserData::UserData(size_t id)
 : buffer(nullptr),
   in_ring(false),
   listener(nullptr),
   conn(nullptr),
   op(Operation::ACCEPT),
   index(id),
   udp_type(UDPType::REQUEST),
   rpc_message(nullptr),
   accept_addr(nullptr) {
}

Buffer* UserData::get_buffer() {
    if (buffer == nullptr) {
        LOG(FATAL) << "Buffer is null"
                     << ", index: " << index
                     << ", op: " << operation_to_str(op)
                     << ", udp_type: " << udp_type_to_str(udp_type)
                     << ", conn: " << conn
                     << ", listener: " << listener;
    }
    return buffer;
}

void UserData::set_buffer(Buffer* buf) {
    if (buf == nullptr) {
        LOG(FATAL) << "Buffer cannot be null";
    }
    buffer = buf;
}

void UserData::clear() {
    buffer = nullptr;
    listener = nullptr;
    conn = nullptr;
    rpc_message.reset();
    op = Operation::CLEAR;
    udp_type = UDPType::CLEAR;
    in_ring = false;
    accept_addr.reset();
}

BufferManager::BufferManager(size_t len, size_t buffer_size)
 : count(len), size(buffer_size) {
    for (size_t i = 0; i < count; i++) {
        buffer_queue.push(new Buffer(size, i));
        user_data_queue.push(new UserData(i));
    }
};

Buffer* BufferManager::get_buffer() {
    if (buffer_queue.empty()) {
        LOG(FATAL) << "No free buffer available";
        return nullptr;
    }
    Buffer* buffer = buffer_queue.front();
    buffer_queue.pop();
    if (!buffer->is_free) {
        LOG(FATAL) << "Buffer is not free, index: " << buffer->get_index();
        return nullptr;
    }
    buffer->is_free = false;
    return buffer;
}

void BufferManager::free_buffer(Buffer*& buffer) {
    if (buffer == nullptr) {
        LOG(FATAL) << "buffer cannot be null";
        return;
    }

    if (buffer->is_free) {
        LOG(FATAL) << "Buffer is already free, index: " << buffer->get_index();
        return;
    }
    buffer->clear();
    buffer_queue.push(buffer);
    buffer = nullptr;
}

UserData* BufferManager::get_user_data() {
    if (user_data_queue.empty()) {
        LOG(FATAL) << "No free user data available";
    }
    UserData* ud = user_data_queue.front();
    user_data_queue.pop();
    VLOG(2) << "get new ud " << ud->index;
    return ud;
}

void BufferManager::free_user_data(UserData*& ud) {
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