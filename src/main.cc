#include <event_loop.h>
#include <config.h>
#include <iostream>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
    // init logging
    google::InitGoogleLogging(argv[0]);

    Config config = {
        1000,
        100,
        4048,
        10001,
        10000
    };

    EventLoop event_loop(config);
    LOG(INFO) << "Starting event loop"; 
    try
    {
        event_loop.run();
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
    
}

