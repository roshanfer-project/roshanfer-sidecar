#include <buffer.h>
#include "ring_helper.h"
#include <buffer_manager.h>
#include <memory>
#include <netinet/in.h>
#include <vector>
#include <listener.h>
#include <glog/logging.h>

BufferManager::BufferManager(int count, int size)
 : count(count), size(size) {
    for (int i = 0; i < count; i++) {
        buffers.push_back(new Buffer(size, i));
        used_buffer.push_back(false);
        used_user_data.push_back(false);
        user_data_vec.push_back(new UserData(
            nullptr,
            nullptr,
            nullptr,
            Operation::ACCEPT,
            i,
            UDPType::REQUEST,
            nullptr
        ));
    }
};

Buffer* BufferManager::get_buffer() {
    for (int i = 0; i < count; i++) {
        if (!used_buffer[i]) {
            used_buffer[i] = true;
            std::memset(buffers[i]->data.get(), 0, size);
            return buffers[i];
        }
    }
    throw std::runtime_error("No free buffers");
}

void BufferManager::free_buffer(Buffer* buffer) {
    used_buffer[buffer->get_index()] = false;
    buffer->clear();
    // Basically we are moving the buffer into this function (the c way!)
    buffer = nullptr;
}

UserData* BufferManager::get_user_data() {
    for (int i = 0; i < count; i++) {
        if (!used_user_data[i]) {
            used_user_data[i] = true;
            return user_data_vec[i];
        }
    }
    LOG(FATAL) << "No free user data";
}

void BufferManager::free_user_data(UserData* ud) {
    ud->buffer = nullptr;
    ud->listener = nullptr;
    ud->conn = nullptr;
    ud->rpc_message = nullptr;
    used_user_data[ud->index] = false;
    ud = nullptr;
}