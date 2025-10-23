#pragma once

#include <cstddef>
#include <memory>
#include <sys/socket.h>
#include "buffer.h"
#include "ring_helper.hpp"

class BufferManager {

public:
    BufferManager(size_t, size_t);
    std::unique_ptr<Buffer> get_buffer();
    void free_buffer(std::unique_ptr<Buffer>);
    UserData* get_user_data();
    void free_user_data(UserData*&);

private:
    size_t count;
    size_t size;
    std::queue<std::unique_ptr<Buffer>> buffer_queue;
    std::queue<UserData*> user_data_queue;
};