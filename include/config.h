#pragma once

#include <cstddef>
#include <cstdint>
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
    size_t ring_size;
    size_t buffer_count;
    size_t buffer_size;
    uint16_t egress_listener_port;
    uint16_t ingress_listener_port;
    std::string ingress_upstream_host;
    uint16_t ingress_upstream_port;
    int ppm_limit;
    std::string name;
    std::vector<RoutingEntry> routing;
    bool is_ingress;
    bool report_latency;
};

Config load_config(const std::string &filename);

extern Config config;