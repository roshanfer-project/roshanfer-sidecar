#pragma once

#include "glog/logging.h"
#include <cstddef>
#include <set>
#include <string_view>
#include <unordered_map>

using ReplicaIndex = int;
using RPCID = int64_t;
using Priority = int32_t;

struct TransparentHash {
  using is_transparent = void; // important for heterogeneous lookup
  std::size_t operator()(std::string_view txt) const noexcept {
    return std::hash<std::string_view>{}(txt);
  }
};

struct TransparentEqual {
  using is_transparent = void;
  bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
    return lhs == rhs;
  }
};

class KeyValueMinTracker {

public:
  // Insert or update an item's value: O(log n)
  void set_value(ReplicaIndex key, int newValue) {
    // If key already exists, erase its old value record from the sorted set
    if (keyToValue.find(key) != keyToValue.end()) {
      int oldValue = keyToValue[key];
      valueToKey.erase({oldValue, key});
    }

    // Insert/update new values in both containers
    keyToValue[key] = newValue;
    valueToKey.insert({newValue, key});
  }

  // Get the item with the minimum value: O(1)
  std::pair<int, ReplicaIndex> get_min() { return *valueToKey.begin(); }

  int increase(ReplicaIndex key) {
    auto it = keyToValue.find(key);
    int val;
    if (it == keyToValue.end()) {
      val = 1;
    } else {
      val = it->second + 1;
    }
    set_value(key, val);
    return val;
  }

  int decrease(ReplicaIndex key) {
    auto it = keyToValue.find(key);
    if (it == keyToValue.end()) {
      LOG(FATAL) << "key not found for decrease";
    }
    if (it->second < 1) {
      LOG(FATAL) << "value should be at least 1 for decrease";
    }
    int val = it->second - 1;
    set_value(key, val);
    return val;
  }

  void init(ReplicaIndex key) {
    if (auto it = keyToValue.find(key); it != keyToValue.end()) {
      LOG(FATAL) << "key already exists";
    } else {
      set_value(key, 0);
    }
  }

private:
  std::unordered_map<ReplicaIndex, int> keyToValue;
  std::set<std::pair<int, ReplicaIndex>> valueToKey;
};