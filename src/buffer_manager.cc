#include "connection.h"
#include <buffer_manager.h>
#include <memory>
#include <vector>
#include <listener.h>
#include <glog/logging.h>

Buffer::Buffer(int size, int index)
 :  size(size),
    data(std::make_unique<char[]>(size)),
    index(index),
    filled(0),
    conn(nullptr),
    listener(nullptr) {
};

BufferManager::BufferManager(int count, int size)
 : count(count), size(size) {
    for (int i = 0; i < count; i++) {
        buffers.push_back(new Buffer(size, i));
        used_buffer.push_back(false);
        used_user_data.push_back(false);
        user_data_vec.push_back(new UserData(
            nullptr, Operation::ACCEPT, i
        ));
    }
};

Buffer* BufferManager::get_buffer(TCPConnection* conn, Listener* listener) {
    for (int i = 0; i < count; i++) {
        if (!used_buffer[i]) {
            used_buffer[i] = true;
            buffers[i]->conn = conn;
            buffers[i]->listener = listener;
            std::memset(buffers[i]->data.get(), 0, size);
            return buffers[i];
        }
    }
    throw std::runtime_error("No free buffers");
}

void BufferManager::free_buffer(int i) {
    used_buffer[i] = false;
}

UserData* BufferManager::get_user_data() {
    for (int i = 0; i < count; i++) {
        if (!used_user_data[i]) {
            used_user_data[i] = true;
            return user_data_vec[i];
        }
    }
    DLOG(FATAL) << "No free user data";
}

void BufferManager::free_user_data(int i) {
    used_user_data[i] = false;
    user_data_vec[i]->data = nullptr;
}