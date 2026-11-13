#include "config.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

namespace fs = std::filesystem;

namespace {

struct Expected {
  bool mesh_late_binding;
  LateBindingType late_binding_type;
  bool concurrent_credit;
};

// ConfigMap file name -> expected late-binding keys of its mode.
const std::map<std::string, Expected> kModes = {
    {"sidecar-configs.yaml", {false, LateBindingType::NONE, false}},
    {"amphiqueue-configs.yaml", {false, LateBindingType::NONE, true}},
    {"amphiqueue-fcfs-configs.yaml", {true, LateBindingType::FCFS, true}},
    {"amphiqueue-edf-configs.yaml", {true, LateBindingType::EDF, true}},
};

const std::string kBase = R"(
ring_size: 1000
buffer_count: 1024
buffer_size: 80000
num_threads: 1
egress_listener_port: 4000
ingress_listener_port: 3000
ingress_upstream_host: localhost
ingress_upstream_port: 2000
name: svc
cpu_count: 1
over_commitment: 1.0
mapping:
  svc:
)";

Config parse(const std::string &yaml) {
  const fs::path path = fs::temp_directory_path() /
                        ("config_test_" + std::to_string(getpid()) + ".yaml");
  std::ofstream(path) << yaml;
  Config parsed = load_config(path.string());
  fs::remove(path);
  return parsed;
}

void expect_mode(const Config &c, const Expected &e) {
  EXPECT_EQ(c.mesh_late_binding, e.mesh_late_binding);
  EXPECT_EQ(c.late_binding_type, e.late_binding_type);
  EXPECT_EQ(c.ingress_concurrent_credit_requests, e.concurrent_credit);
}

} // namespace

TEST(ConfigTest, PerModeValues) {
  const std::map<std::string, std::string> keys = {
      {"sidecar-configs.yaml", "mesh_late_binding: False\n"
                               "late_binding_type: none\n"
                               "ingress_concurrent_credit_requests: False\n"},
      {"amphiqueue-configs.yaml", "mesh_late_binding: False\n"
                                  "late_binding_type: none\n"
                                  "ingress_concurrent_credit_requests: True\n"},
      {"amphiqueue-fcfs-configs.yaml",
       "mesh_late_binding: True\n"
       "late_binding_type: fcfs\n"
       "ingress_concurrent_credit_requests: True\n"},
      {"amphiqueue-edf-configs.yaml",
       "mesh_late_binding: True\n"
       "late_binding_type: edf\n"
       "ingress_concurrent_credit_requests: True\n"},
  };
  for (const auto &[mode, extra] : keys) {
    SCOPED_TRACE(mode);
    expect_mode(parse(kBase + extra), kModes.at(mode));
  }
}

TEST(ConfigTest, MissingKeysUseDefaults) {
  expect_mode(parse(kBase), {false, LateBindingType::FCFS, false});
}

TEST(ConfigDeathTest, LateBindingWithNoneType) {
  EXPECT_DEATH(parse(kBase + "mesh_late_binding: True\n"
                             "late_binding_type: none\n"),
               "late_binding_type is none");
}

TEST(ConfigDeathTest, UnknownLateBindingType) {
  EXPECT_DEATH(parse(kBase + "late_binding_type: bogus\n"),
               "Invalid late_binding_type");
}

TEST(ConfigTest, RealConfigMaps) {
  const char *root = std::getenv("SIDECAR_CONFIGMAP_ROOT");
  if (root == nullptr) {
    GTEST_SKIP() << "SIDECAR_CONFIGMAP_ROOT not set";
  }
  int files = 0;
  int entries = 0;
  for (auto it = fs::recursive_directory_iterator(root);
       it != fs::recursive_directory_iterator(); ++it) {
    if (it->is_directory() && it->path().filename() == "sidecar") {
      it.disable_recursion_pending();
      continue;
    }
    const auto mode = kModes.find(it->path().filename().string());
    if (mode == kModes.end()) {
      continue;
    }
    ++files;
    for (const auto &entry : YAML::LoadFile(it->path().string())["data"]) {
      SCOPED_TRACE(it->path().string() + " : " +
                   entry.first.as<std::string>());
      expect_mode(parse(entry.second.as<std::string>()), mode->second);
      ++entries;
    }
  }
  std::cout << "Checked " << files << " ConfigMap files, " << entries
            << " entries\n";
  EXPECT_GT(files, 0);
}
