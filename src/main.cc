#include <event_loop.h>
#include <config.h>
#include <iostream>
#include <iomanip>
#include <glog/logging.h>

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

    Config config = load_config(argv[1]);

    EventLoop event_loop(config);
    LOG(INFO) << "Starting event loop"; 
    try
    {
        event_loop.run();
    }
    catch(const std::exception& e)
    {
        DLOG(FATAL) << "Error in event loop: " << e.what();
    }
}

