#pragma once

#include <memory>
#include <vector>
#include <connection.h>
#include <listener.h>

enum Operation {
    ACCEPT,
    READ,
    WRITE,
    CONNECT
};

struct UserData {
    void* data;
    enum Operation op;
    int index;
};

class Buffer {

public:
    Buffer(int, int);
    int get_size() { return size; }

public:
    std::unique_ptr<char[]> data;
    int size;
    int index;
    TCPConnection* conn;
    Listener* listener;
};

class BufferManager {

public:
    BufferManager(int, int);
    Buffer* get_buffer(TCPConnection*, Listener*);
    void free_buffer(int);
    UserData* get_user_data();
    void free_user_data(int);

private:
    int count;
    int size;
    std::vector<Buffer*> buffers;
    std::vector<UserData*> user_data_vec;
    std::vector<bool> used_buffer;
    std::vector<bool> used_user_data;
};