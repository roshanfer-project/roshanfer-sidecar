#pragma once

#include <cstdint>

class QueueMultiplxer {
    public:
        QueueMultiplxer(uint16_t);
        int get_fd() { return fd; }

    private:
        int fd;
};