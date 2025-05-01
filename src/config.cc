#include "config.h"
#include <yaml-cpp/yaml.h>
#include "glog/logging.h"


Config load_config(const std::string &filename) {
    Config config;

    // default values
    config.disable_ingress = false;

    std::string config_path = filename;
    YAML::Node node;
    try {
        node = YAML::LoadFile(config_path);
   } catch (const YAML::BadFile& e) {
        LOG(FATAL) << "Error loading config file: " << config_path << " - " << e.what();
   } catch (const YAML::ParserException& e) {
        LOG(FATAL) << "Error parsing config file: " << config_path << " - " << e.what();
   }

    config.ring_size            = node["ring_size"].as<int>();
    config.buffer_count         = node["buffer_count"].as<int>();
    config.buffer_size          = node["buffer_size"].as<int>();
    config.egress_listener_port = node["egress_listener_port"].as<int>();
    config.ingress_listener_port= node["ingress_listener_port"].as<int>();
    config.ingress_upstream_host= node["ingress_upstream_host"].as<std::string>();
    config.ingress_upstream_port= node["ingress_upstream_port"].as<int>();
    config.ppm_limit            = node["ppm_limit"].as<int>();
    config.name                 = node["name"].as<std::string>();

    // Optional boolean value
    if (node["disable_ingress"]) {
        config.disable_ingress = node["disable_ingress"].as<bool>();
    }

    // Parse the routing section
    if (!node["routing"]) {
        LOG(WARNING) << "Routing section not found";
        return config;
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
                    config.routing.push_back(entry);
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

    return config;
}