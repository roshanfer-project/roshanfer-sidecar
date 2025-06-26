#pragma once

//#include "listener.h"
#include <memory>

class Buffer {

    public:
        Buffer(int, int);
        int get_size() { return size; }
        int get_filled() { return filled; }
        int get_index() { return index; }
        //HTTPConnection& get_conn() { return *conn; }
        //Listener& get_listener() { return *listener; }
        void set_filled(int f);
        //void prepare_read(HTTPConnection*, Listener*);
        //void prepare_write(HTTPConnection*);
        void prepare_recvmsg();
        std::unique_ptr<struct msghdr>& get_msg() { return msg; }
        void prepare_reply_sendmsg(Buffer* old_buffer);
        void prepare_req_sendmsg(struct sockaddr_in);
        void clear();
    
    public:
        std::unique_ptr<char[]> data;
    
    private:
        int size;
        int filled;
        int index;
        //HTTPConnection* conn;
        //Listener* listener;
        std::unique_ptr<struct msghdr> msg;
        std::unique_ptr<struct sockaddr_in> addr;
        std::unique_ptr<struct iovec> iov;
    };