#include <event_loop.h>
#include <config.h>
#include <iostream>
#include <execinfo.h>
#include <cxxabi.h>
#include <glog/logging.h>

void print_stacktrace() {
    constexpr int max_frames = 100;
    void* addrlist[max_frames];

    // Capture the stack trace
    int addrlen = backtrace(addrlist, max_frames);
    if (addrlen == 0) {
        std::cerr << "No stack trace available\n";
        return;
    }

    // Resolve symbols
    char** symbols = backtrace_symbols(addrlist, addrlen);
    if (!symbols) {
        std::cerr << "Failed to resolve stack trace symbols\n";
        return;
    }

    for (int i = 0; i < addrlen; i++) {
        char* mangled_name = nullptr;
        char* offset_begin = nullptr;
        char* offset_end = nullptr;

        // Find parentheses and + sign
        for (char* p = symbols[i]; *p; ++p) {
            if (*p == '(') {
                mangled_name = p + 1;
            } else if (*p == '+') {
                offset_begin = p;
            } else if (*p == ')' && offset_begin) {
                offset_end = p;
                break;
            }
        }

        if (mangled_name && offset_begin && offset_end) {
            *offset_begin = '\0';
            *offset_end = '\0';

            int status;
            char* demangled_name = abi::__cxa_demangle(mangled_name, nullptr, nullptr, &status);

            if (status == 0) {
                std::cerr << symbols[i] << ": " << demangled_name << " + " << offset_begin + 1 << '\n';
            } else {
                std::cerr << symbols[i] << ": " << mangled_name << " + " << offset_begin + 1 << '\n';
            }
            free(demangled_name);
        } else {
            std::cerr << symbols[i] << '\n';
        }
    }

    free(symbols);
}

int main(int argc, char* argv[]) {
    // init logging
    google::InitGoogleLogging(argv[0]);

    Config config = {
        1000, // ring size
        100, // buffer count
        4048, // buffer size
        10000 // listen port
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
        //print_stacktrace();
    }
    
    
}

