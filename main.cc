#include <event_loop.h>
#include <config.h>
#include <iostream>


int main() {
    Config config = {
        100, // ring size
        100, // buffer count
        4048, // buffer size
        10000 // listen port
    };

    EventLoop event_loop(config);
    std::cout << "Starting event loop" << std::endl;
    event_loop.run();
}