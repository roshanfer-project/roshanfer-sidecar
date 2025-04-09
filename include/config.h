#pragma once

#include <string>

struct Config
{
    int ring_size;
    int buffer_count;
    int buffer_size;
    std::string endpoint_host;
    int endpoint_port;
    int egress_listener_port;
    int ingress_listener_port;
    std::string ingress_upstream_host;
    int ingress_upstream_port;
    int ppm_limit;
    std::string name;
};

Config load_config(const std::string &filename);