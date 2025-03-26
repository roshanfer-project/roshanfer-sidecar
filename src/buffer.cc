#include "buffer.h"
#include <bits/types/struct_iovec.h>
#include <cstring>
#include <memory>
#include <netinet/in.h>

Buffer::Buffer(int size, int index)
 :  size(size),
    data(std::make_unique<char[]>(size)),
    index(index),
    filled(0),
    msg(nullptr),
    addr(nullptr),
    iov(nullptr) {
}

void Buffer::clear() {
    std::memset(data.get(), 0, size);
    msg = nullptr;
    addr = nullptr;
    iov = nullptr;
    filled = 0;
}

void Buffer::prepare_recvmsg() {
    iov = std::make_unique<struct iovec>();
    iov->iov_base = data.get();
    iov->iov_len = get_size();

    addr = std::make_unique<struct sockaddr_in>();
    msg = std::make_unique<struct msghdr>();

    msg->msg_name = addr.get();
    msg->msg_namelen = sizeof(struct sockaddr_in);
    msg->msg_iov = iov.get();
    msg->msg_iovlen = 1;
}

void Buffer::prepare_reply_sendmsg(Buffer* old_buffer) {
    iov = std::make_unique<struct iovec>();
    iov->iov_base = data.get();
    iov->iov_len = filled;

    addr = std::move(old_buffer->addr);
    msg = std::make_unique<struct msghdr>();
    msg->msg_name = addr.get(); 
    msg->msg_namelen = sizeof(struct sockaddr_in);
    msg->msg_iov = iov.get();
    msg->msg_iovlen = 1;
}

void Buffer::prepare_req_sendmsg(struct sockaddr_in servaddr) {
    iov = std::make_unique<struct iovec>();
    iov->iov_base = data.get();
    iov->iov_len = filled;

    addr = std::make_unique<struct sockaddr_in>(servaddr);
    msg = std::make_unique<struct msghdr>();
    msg->msg_name = addr.get(); 
    msg->msg_namelen = sizeof(struct sockaddr_in);
    msg->msg_iov = iov.get();
    msg->msg_iovlen = 1;
}