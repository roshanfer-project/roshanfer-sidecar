#pragma once

#include "utils.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

struct Upstream {
  std::string host;
  int port;
};

struct RoutingEntry {
  Upstream upstream;
  std::optional<int> slo;
  std::optional<int32_t> priority;
};

struct MappingInfo {
  std::vector<std::string> downstreams;
  std::optional<uint16_t> listen_port;
  std::optional<bool> pfanout;
  std::optional<bool> dfanout;
};

struct Config {
  size_t ring_size;
  size_t buffer_count;
  size_t buffer_size;
  int num_threads;
  uint16_t egress_listener_port;
  uint16_t ingress_listener_port;
  std::string ingress_upstream_host;
  uint16_t ingress_upstream_port;
  std::string name;
  std::unordered_map<std::string, RoutingEntry, TransparentHash,
                     TransparentEqual>
      routing;
  std::unordered_map<std::string, MappingInfo, TransparentHash,
                     TransparentEqual>
      mapping;
  bool is_ingress;
  bool is_frontend;
  bool is_plain_frontend;
  std::optional<int> ingress_pool_connections;
  std::optional<int> frontend_pool_connections;
  std::optional<int> cpu_count;
  std::optional<float> over_commitment;
  int extra_limit;
};

Config load_config(const std::string &filename);

extern Config config;

inline std::vector<std::string>
get_downstream_services(const Config &local_config) {
  std::vector<std::string> downstream_services;
  for (const auto &[route, _] : local_config.routing) {
    downstream_services.push_back(route);
  }
  return downstream_services;
}

inline std::vector<std::string>
get_hosted_services(const Config &local_config) {
  std::vector<std::string> hosted_services;
  for (const auto &mapping : local_config.mapping) {
    hosted_services.push_back(mapping.first);
  }
  return hosted_services;
}