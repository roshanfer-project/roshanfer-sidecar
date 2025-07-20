#include "config.h"
#include <cstddef>
#include <cstdint>
#include <yaml-cpp/yaml.h>
#include "glog/logging.h"

Config config;

Config load_config(const std::string &filename) {
    Config local_config;

    std::string config_path = filename;
    YAML::Node node;
    try {
        node = YAML::LoadFile(config_path);
   } catch (const YAML::BadFile& e) {
        LOG(FATAL) << "Error loading config file: " << config_path << " - " << e.what();
   } catch (const YAML::ParserException& e) {
        LOG(FATAL) << "Error parsing config file: " << config_path << " - " << e.what();
   }

    local_config.ring_size            = node["ring_size"].as<size_t>();
    local_config.buffer_count         = node["buffer_count"].as<size_t>();
    local_config.buffer_size          = node["buffer_size"].as<size_t>();
    local_config.egress_listener_port = node["egress_listener_port"].as<uint16_t>();
    local_config.ingress_listener_port= node["ingress_listener_port"].as<uint16_t>();
    local_config.ingress_upstream_host= node["ingress_upstream_host"].as<std::string>();
    local_config.ingress_upstream_port= node["ingress_upstream_port"].as<uint16_t>();
    local_config.ppm_limit            = node["ppm_limit"].as<int>();
    local_config.name                 = node["name"].as<std::string>();

    LOG(INFO) << "config.name: " << local_config.name;

    // Optional is_ingress
    if (node["is_ingress"]) {
        local_config.is_ingress = node["is_ingress"].as<bool>();
    } else {
        local_config.is_ingress = false;
    }
    LOG(INFO) << "config.is_ingress: " << local_config.is_ingress;

    // Optional report_latency
    if (node["report_latency"]) {
        local_config.report_latency = node["report_latency"].as<bool>();
    } else {
        local_config.report_latency = false;
    }
    LOG(INFO) << "config.report_latency: " << local_config.report_latency;

    // Optional is_frontend
    if (node["is_frontend"]) {
        local_config.is_frontend = node["is_frontend"].as<bool>();
    } else {
        local_config.is_frontend = false;
    }
    LOG(INFO) << "config.is_frontend: " << local_config.is_frontend;

    // Parse the routing section
    if (!node["routing"]) {
        LOG(WARNING) << "Routing section not found";
        config = local_config;
        return local_config;
    }
    if (node["routing"].IsSequence()) {
        for (const auto& route_node : node["routing"]) {
            RoutingEntry entry;
            if (route_node["service"] && route_node["upstream"]) {
                entry.service = route_node["service"].as<std::string>();
                const auto& upstream_node = route_node["upstream"];
                if (upstream_node["host"] && upstream_node["port"]) {
                    entry.upstream.host = upstream_node["host"].as<std::string>();
                    entry.upstream.port = upstream_node["port"].as<int>();
                    local_config.routing.push_back(entry);
                } else {
                    LOG(FATAL) << "Skipping routing entry: Missing host or port in upstream section for service " << entry.service;
                }
            } else {
                LOG(FATAL) << "Skipping routing entry: Missing service or upstream section.";
            }
    }
    } else {
        LOG(FATAL) << "Routing section not a sequence in " << config_path;
    }

    config = local_config;
    return local_config;
}