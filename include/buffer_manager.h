#pragma once

#include <cstddef>
#include <sys/socket.h>
#include "buffer.h"
#include "ring_helper.h"

class BufferManager {

public:
    BufferManager(size_t, size_t);
    Buffer* get_buffer();
    void free_buffer(Buffer*&);
    UserData* get_user_data();
    void free_user_data(UserData*&);

private:
    size_t count;
    size_t size;
    std::queue<Buffer*> buffer_queue;
    std::queue<UserData*> user_data_queue;
};