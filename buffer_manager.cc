#include <buffer_manager.h>
#include <memory>
#include <vector>
#include <listener.h>

Buffer::Buffer(int size, int index)
 : size(size), data(std::make_unique<char[]>(size)), index(index) {
};

BufferManager::BufferManager(int count, int size)
 : count(count), size(size) {
    for (int i = 0; i < count; i++) {
        buffers.push_back(std::make_unique<Buffer>(size, i));
        used.push_back(false);
    }
};

const std::unique_ptr<Buffer>& BufferManager::get_buffer(Listener* listener) {
    for (int i = 0; i < count; i++) {
        if (!used[i]) {
            used[i] = true;
            buffers[i].get()->listener = listener;
            return buffers[i];
        }
    }
    throw std::runtime_error("No free buffers");
}

void BufferManager::free_buffer(int i) {
    used[i] = false;
}