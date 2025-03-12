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
    std::memset(buffer->data.get(), 0, buffer->get_size());
    buffer->set_filled(0);
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
    used_user_data[ud->index] = false;
    user_data_vec[ud->index]->data = nullptr;
    ud = nullptr;
}

void Buffer::prepare_read(HTTPConnection* c, Listener* l) {
    conn = c;
    listener = l;
}

void Buffer::prepare_write(HTTPConnection* c) {
    conn = c;
}