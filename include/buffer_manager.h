#pragma once

#include <memory>
#include <vector>
#include <connection.h>
#include <listener.h>

class Buffer {

public:
    Buffer(int, int);
    int get_size() { return size; }

public:
    std::unique_ptr<char[]> data;
    int size;
    int index;
    Listener* listener;
};

class BufferManager {

public:
    BufferManager(int, int);
    const std::unique_ptr<Buffer>& get_buffer(Listener*);
    void free_buffer(int);

private:
    int count;
    int size;
    std::vector<std::unique_ptr<Buffer>> buffers;
    std::vector<bool> used;
};