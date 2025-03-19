#pragma once

#include <sys/socket.h>
#include <vector>
#include <connection.h>
#include <listener.h>
#include "buffer.h"
#include "ring_helper.h"

class BufferManager {

public:
    BufferManager(int, int);
    Buffer* get_buffer();
    void free_buffer(Buffer*);
    UserData* get_user_data();
    void free_user_data(UserData*);

private:
    int count;
    int size;
    std::vector<Buffer*> buffers;
    std::vector<UserData*> user_data_vec;
    std::vector<bool> used_buffer;
    std::vector<bool> used_user_data;
};