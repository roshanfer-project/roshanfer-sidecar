#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include "utils.h"

struct Upstream {
    std::string host;
    int port;
};

struct RoutingEntry {
    Upstream upstream;
    std::optional<int> ingress_limit;
    std::optional<int> slo;
};

struct MappingInfo {
    std::vector<std::string> downstreams;
    std::optional<int> min_max_concurrency;
    int limit;
    std::optional<uint16_t> listen_port;
};

struct Config
{
    size_t ring_size;
    size_t buffer_count;
    size_t buffer_size;
    int num_threads;
    uint16_t egress_listener_port;
    uint16_t ingress_listener_port;
    std::string ingress_upstream_host;
    uint16_t ingress_upstream_port;
    int ppm_limit;
    std::string name;
    std::unordered_map<std::string, RoutingEntry, TransparentHash, TransparentEqual> routing;
    std::unordered_map<std::string, MappingInfo, TransparentHash, TransparentEqual> mapping;
    bool is_ingress;
    bool is_frontend;
    bool report_latency;
};

Config load_config(const std::string &filename);

extern Config config;