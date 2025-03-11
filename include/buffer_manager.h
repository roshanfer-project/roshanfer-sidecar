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

enum ReqRes {
    REQUEST,
    // RESPONE basically means that this message should be just forwarded 
    // back to the sneder without any routing
    RESPONSE
};

struct UserData {
    void* data;
    enum Operation op;
    int index;
    enum ReqRes req_res;
};

class Buffer {

public:
    Buffer(int, int);
    int get_size() { return size; }
    int get_filled() { return filled; }
    int get_index() { return index; }
    HTTPConnection& get_conn() { return *conn; }
    Listener& get_listener() { return *listener; }
    void set_filled(int f) { filled = f; }
    void prepare_read(HTTPConnection*, Listener*);
    void prepare_write(HTTPConnection*);

public:
    std::unique_ptr<char[]> data;

private:
    int size;
    int filled;
    int index;
    HTTPConnection* conn;
    Listener* listener;
};

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