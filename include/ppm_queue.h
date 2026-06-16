#pragma once

#include "config.h"
#include "fast_map.hpp"
#include "rpc_message.h"
#include "utils.h"
#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unordered_map>

class ReplicaWaiting {

public:
  ReplicaWaiting(ReplicaIndex replica_index) : index(replica_index) {};

  bool operator>(const ReplicaWaiting &other) const {
    return count > other.count;
  }

public:
  ReplicaIndex index;
  int count{};
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

class PPMQueue {
public:
  PPMQueue(std::unordered_map<std::string, RoutingEntry, TransparentHash,
                              TransparentEqual>
               routing);
  void push(std::shared_ptr<RPCMessage> rpc);
  std::shared_ptr<RPCMessage> pop(const std::string &service, RPCID id);
  // checks if the service exists and the queue is not empty
  const std::string &check(std::string_view &);
  size_t size(const std::string &);
  int32_t get_waiting_delay_us(const std::string &service);

public:
  std::unordered_map<std::string, KeyValueMinTracker, TransparentHash,
                     TransparentEqual>
      replica_waiting_count{};

private:
  std::unordered_map<std::string, std::map<RPCID, std::shared_ptr<RPCMessage>>,
                     TransparentHash, TransparentEqual>
      ppm_queue;
};
