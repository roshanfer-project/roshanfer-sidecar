#include "config.h"
#include "glog/logging.h"
#include <cstddef>
#include <cstdint>
#include <yaml-cpp/yaml.h>

Config config;

Config load_config(const std::string &filename) {
  Config local_config;

  std::string config_path = filename;
  YAML::Node node;
  try {
    node = YAML::LoadFile(config_path);
  } catch (const YAML::BadFile &e) {
    LOG(FATAL) << "Error loading config file: " << config_path << " - "
               << e.what();
  } catch (const YAML::ParserException &e) {
    LOG(FATAL) << "Error parsing config file: " << config_path << " - "
               << e.what();
  }

  local_config.ring_size = node["ring_size"].as<size_t>();
  local_config.buffer_count = node["buffer_count"].as<size_t>();
  local_config.buffer_size = node["buffer_size"].as<size_t>();
  local_config.num_threads = node["num_threads"].as<int>();
  local_config.egress_listener_port =
      node["egress_listener_port"].as<uint16_t>();
  local_config.ingress_listener_port =
      node["ingress_listener_port"].as<uint16_t>();
  local_config.ingress_upstream_host =
      node["ingress_upstream_host"].as<std::string>();
  local_config.ingress_upstream_port =
      node["ingress_upstream_port"].as<uint16_t>();
  local_config.name = node["name"].as<std::string>();

  LOG(INFO) << "config.name: " << local_config.name;

  // Optional is_ingress
  if (node["is_ingress"]) {
    local_config.is_ingress = node["is_ingress"].as<bool>();
  } else {
    local_config.is_ingress = false;
  }
  LOG(INFO) << "config.is_ingress: " << local_config.is_ingress;

  if (node["is_plain_frontend"]) {
    local_config.is_plain_frontend = node["is_plain_frontend"].as<bool>();
  } else {
    local_config.is_plain_frontend = false;
  }

  LOG(INFO) << "config.is_plain_frontend: " << local_config.is_plain_frontend;

  // Optional report_latency
  if (node["report_latency"]) {
    local_config.report_latency = node["report_latency"].as<bool>();
  } else {
    local_config.report_latency = false;
  }
  LOG(INFO) << "config.report_latency: " << local_config.report_latency;

  // Optional over_commitment
  if (!local_config.is_ingress && !node["over_commitment"]) {
    LOG(FATAL) << "over_commitment is required for non-ingress";
  }
  if (!local_config.is_ingress) {
    local_config.over_commitment = node["over_commitment"].as<float>();
    if (local_config.over_commitment > 1 || local_config.over_commitment < 0) {
      LOG(FATAL) << "over_commitment must be between 0 and 1";
    }
  }

  // Optional cpu_count
  if (!local_config.is_ingress && !node["cpu_count"]) {
    LOG(FATAL) << "cpu_count is required for non-ingress";
  }
  if (!local_config.is_ingress) {
    local_config.cpu_count = node["cpu_count"].as<int>();
    if (local_config.cpu_count < 1) {
      LOG(FATAL) << "cpu_count must be at least 1";
    }
  }

  if (node["extra_limit"]) {
    local_config.extra_limit = node["extra_limit"].as<int>();
  } else {
    local_config.extra_limit = 0;
  }
  LOG(INFO) << "config.extra_limit: " << local_config.extra_limit;

  // Optional is_frontend
  if (node["is_frontend"]) {
    local_config.is_frontend = node["is_frontend"].as<bool>();
  } else {
    local_config.is_frontend = false;
  }
  LOG(INFO) << "config.is_frontend: " << local_config.is_frontend;

  // ingress_pool_connections (required if is_ingress, optional otherwise)
  if (node["ingress_pool_connections"]) {
    local_config.ingress_pool_connections =
        node["ingress_pool_connections"].as<int>();
  } else if (local_config.is_ingress) {
    LOG(FATAL)
        << "ingress_pool_connections is required when is_ingress is true";
  }
  LOG(INFO) << "config.ingress_pool_connections: "
            << (local_config.ingress_pool_connections.has_value()
                    ? std::to_string(
                          local_config.ingress_pool_connections.value())
                    : "not set");

  // frontend_pool_connections (required if is_frontend, optional otherwise)
  if (node["frontend_pool_connections"]) {
    local_config.frontend_pool_connections =
        node["frontend_pool_connections"].as<int>();
  } else if (local_config.is_frontend) {
    LOG(FATAL)
        << "frontend_pool_connections is required when is_frontend is true";
  }
  LOG(INFO) << "config.frontend_pool_connections: "
            << (local_config.frontend_pool_connections.has_value()
                    ? std::to_string(
                          local_config.frontend_pool_connections.value())
                    : "not set");

  // Parse the routing section
  if (node["routing"]) {
    if (node["routing"].IsMap()) {
      for (const auto &route_node : node["routing"]) {
        std::string service = route_node.first.as<std::string>();
        RoutingEntry entry;

        const auto &route_config = route_node.second;
        if (route_config["upstream"]) {
          const auto &upstream_node = route_config["upstream"];
          if (upstream_node["host"] && upstream_node["port"]) {
            entry.upstream.host = upstream_node["host"].as<std::string>();
            entry.upstream.port = upstream_node["port"].as<int>();

            if (route_config["slo"]) {
              entry.slo = route_config["slo"].as<int>();
            }

            if (route_config["priority"]) {
              entry.priority = route_config["priority"].as<int>();
            }

            local_config.routing[service] = entry;
            LOG(INFO) << "Routing entry: service=" << service
                      << " upstream=" << entry.upstream.host << ":"
                      << entry.upstream.port << " slo="
                      << (entry.slo.has_value()
                              ? std::to_string(entry.slo.value())
                              : "not set")
                      << " priority="
                      << (entry.priority.has_value()
                              ? std::to_string(entry.priority.value())
                              : "not set");
          } else {
            LOG(FATAL) << "Skipping routing entry: Missing host or port in "
                          "upstream section for service "
                       << service;
          }
        } else {
          LOG(FATAL)
              << "Skipping routing entry: Missing upstream section for service "
              << service;
        }
      }
    } else {
      LOG(FATAL) << "Routing section is not a map in " << config_path;
    }
  } else {
    LOG(WARNING) << "Routing section not found (optional)";
  }

  // Parse the mapping section
  if (node["mapping"]) {
    if (node["mapping"].IsMap()) {
      for (const auto &mapping_node : node["mapping"]) {
        std::string upstream = mapping_node.first.as<std::string>();
        MappingInfo mapping_info;

        if (mapping_node.second["downstreams"]) {
          if (mapping_node.second["downstreams"].IsSequence()) {
            for (const auto &downstream : mapping_node.second["downstreams"]) {
              mapping_info.downstreams.push_back(downstream.as<std::string>());
            }
            if (mapping_info.downstreams.size() > 10) {
              LOG(FATAL) << "Maximum downstream queue for dfanout is 10";
            }
            if (mapping_info.downstreams.size() > 255) {
              LOG(FATAL) << "For parallel fan-out we use uint8 for counting";
            }
          } else {
            LOG(FATAL) << "Downstreams for upstream '" << upstream
                       << "' is not a sequence in " << config_path;
          }
        }

        if (mapping_node.second["listen_port"]) {
          mapping_info.listen_port =
              mapping_node.second["listen_port"].as<uint16_t>();
        }

        if (mapping_node.second["pfanout"]) {
          mapping_info.pfanout = mapping_node.second["pfanout"].as<bool>();
        }

        if (mapping_node.second["dfanout"]) {
          mapping_info.dfanout = mapping_node.second["dfanout"].as<bool>();
        }

        if (local_config.is_ingress) {
          if (mapping_info.downstreams.size() != 1) {
            LOG(FATAL) << "1:1 mapping violation for Ingress service: "
                       << upstream;
          }
          if (mapping_info.dfanout || mapping_info.pfanout) {
            LOG(FATAL) << "pfanout or dfanout is true for Ingress service: "
                       << upstream;
          }
        }

        local_config.mapping[upstream] = mapping_info;
      }
    } else {
      LOG(FATAL) << "Mapping section is not a map in " << config_path;
    }
  } else {
    LOG(INFO) << "Mapping section not found (optional)";
  }

  if (!local_config.is_ingress) {
    LOG(INFO) << "config.over_commitment: "
              << local_config.over_commitment.value();
    LOG(INFO) << "config.cpu_count: " << local_config.cpu_count.value();
  }

  if (config.is_ingress &&
      (local_config.num_threads != (int)local_config.mapping.size())) {
    LOG(FATAL) << "Number of threads (" << local_config.num_threads
               << ") does not match the number of hosted services ("
               << local_config.mapping.size() << ")";
  }

  if (local_config.num_threads > 1 && !local_config.is_ingress) {
    LOG(FATAL) << "Cannot set number of threads for non-ingress sidecars "
                  "higher than 1 because of RPC ID map that is local to "
                  "threads (fix that before removing this conditions)";
  }

  // Log parsed mapping configuration
  for (const auto &pair : local_config.mapping) {
    LOG(INFO) << "Mapping: upstream=" << pair.first;
    LOG(INFO) << "  listen_port: "
              << (pair.second.listen_port.has_value()
                      ? std::to_string(pair.second.listen_port.value())
                      : "not set");
    for (const auto &downstream : pair.second.downstreams) {
      LOG(INFO) << "  downstream: " << downstream;
    }
  }

  config = local_config;
  return local_config;
}