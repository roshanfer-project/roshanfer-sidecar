#pragma once

#include <cstdint>

class UDPListner {
    public:
        UDPListner(uint16_t);
        int get_fd() { return fd; }

    private:
        int fd;
};