#include "config.h"
#include <yaml-cpp/yaml.h>
#include "glog/logging.h"


Config load_config(const std::string &filename) {
    Config config;
    YAML::Node node = YAML::LoadFile("../" + filename);
    config.ring_size            = node["ring_size"].as<int>();
    config.buffer_count         = node["buffer_count"].as<int>();
    config.buffer_size          = node["buffer_size"].as<int>();
    config.endpoint_host        = node["endpoint_host"].as<std::string>();
    config.endpoint_port        = node["endpoint_port"].as<int>();
    config.egress_listener_port = node["egress_listener_port"].as<int>();
    config.ingress_listener_port= node["ingress_listener_port"].as<int>();
    config.ingress_upstream_host= node["ingress_upstream_host"].as<std::string>();
    config.ingress_upstream_port= node["ingress_upstream_port"].as<int>();
    return config;
}