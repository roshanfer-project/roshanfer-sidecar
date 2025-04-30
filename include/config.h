#pragma once

#include <string>
#include <vector>

struct Upstream {
    std::string host;
    int port;
};

struct RoutingEntry {
    std::string service;
    Upstream upstream;
};

struct Config
{
    int ring_size;
    int buffer_count;
    int buffer_size;
    int egress_listener_port;
    int ingress_listener_port;
    std::string ingress_upstream_host;
    int ingress_upstream_port;
    int ppm_limit;
    std::string name;
    std::vector<RoutingEntry> routing;
};

Config load_config(const std::string &filename);