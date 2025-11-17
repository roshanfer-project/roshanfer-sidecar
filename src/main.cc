#include "state.h"
#include <cstddef>
#include <cstdint>
#include <event_loop.h>
#include <config.h>
#include <iostream>
#include <iomanip>
#include <glog/logging.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <pthread.h>

void MyPrefixFormatter(std::ostream& s, const google::LogMessage& m, void* /*data*/) {
    s << google::GetLogSeverityName(m.severity())[0]
    << ' '
    << std::setw(2) << m.time().hour() << ':'
    << std::setw(2) << m.time().min()  << ':'
    << std::setw(2) << m.time().sec() << "."
    << std::setw(6) << m.time().usec()
    << ' '
    << m.basename() << ':' << m.line() << "]";
}
 

int main(int argc, char* argv[]) {
    // check one required argument
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    // init logging
    google::InitGoogleLogging(argv[0]);
    google::InstallPrefixFormatter(&MyPrefixFormatter);
    google::InstallFailureSignalHandler();

    Config parsed_config = load_config(argv[1]);

    std::vector<std::string> hosted_services;
    for (const auto& [service, _] : parsed_config.mapping) {
        hosted_services.push_back(service);
    }

    std::vector<std::string> downstream_services;
    for (const auto& [route, _] : parsed_config.routing) {
        downstream_services.push_back(route);
    }

    if (hosted_services.size() != (size_t)parsed_config.num_threads && parsed_config.is_ingress) {
        LOG(FATAL) << "Number of hosted services (" << hosted_services.size() << ") does not match the number of threads (" << parsed_config.num_threads << ")";
    }

    if (downstream_services.size() != hosted_services.size() && parsed_config.is_ingress) {
        LOG(FATAL) << "Number of downstream services (" << downstream_services.size() << ") does not match the number of hosted services (" << hosted_services.size() << ")";
    }

    SharedState shared_state = SharedState(hosted_services, downstream_services);

    // create threads for each event loop
    std::vector<std::thread> threads;
    for (uint8_t i = 0; i < parsed_config.num_threads; i++) {
        threads.emplace_back([&shared_state, parsed_config, i, &downstream_services]() {
            // Set thread name for identification in htop
            std::string thread_name = parsed_config.name + "-s-" + std::to_string(i);
            int ret = pthread_setname_np(pthread_self(), thread_name.c_str());
            if (ret != 0) {
                LOG(FATAL) << "Failed to set thread name: " << strerror(ret);
            }
            std::string ingress_service = parsed_config.is_ingress ? downstream_services.at(i) : "empty";

            try {
                EventLoop event_loop(i, ingress_service, parsed_config, shared_state);
                LOG(INFO) << "Starting event loop with index: " << (int)i;
                event_loop.run();
            } catch (const std::out_of_range& e) {
                LOG(FATAL) << "Out of range error: " << e.what();
            } catch (const std::runtime_error& e) {
                LOG(FATAL) << "Runtime error: " << e.what();
            } catch(const std::exception& e)
            {
                LOG(FATAL) << "Error in event loop: " << e.what();
            }
        });
    }

    // wait for all threads to finish
    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

