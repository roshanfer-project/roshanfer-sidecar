#include <event_loop.h>
#include <config.h>
#include <iostream>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
    // check one required argument
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }
    // init logging
    google::InitGoogleLogging(argv[0]);

    Config config = load_config(argv[1]);

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

