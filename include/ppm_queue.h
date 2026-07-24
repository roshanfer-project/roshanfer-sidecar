#pragma once

#include "config.h"
#include "utils.h"
#include <cstddef>
#include <cstdint>
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

struct PriorityId {
  int64_t priority;
  RPCID id;

  // Priority primary; id is tie-break for std::set uniqueness only.
  bool operator<(const PriorityId &o) const {
    if (priority != o.priority) {
      return priority < o.priority;
    }
    return id < o.id;
  }
};

class FlexiblePriorityQueue {
public:
  RPCID pop() {
    if (priority_id_set.empty()) {
      LOG(FATAL) << "FlexiblePriorityQueue is empty";
    }
    auto item = *priority_id_set.begin();
    pop(item.id);
    return item.id;
  }

  RPCID pop(RPCID id) {
    if (auto it = id_to_priority.find(id); it != id_to_priority.end()) {
      priority_id_set.erase(PriorityId{it->second, id});
      id_to_priority.erase(it);
      return id;
    } else {
      LOG(FATAL) << "could not find id: " << id << " in id_to_priority";
    }
  }

  void push(RPCID id, int64_t priority) {
    auto [_, ok] = id_to_priority.emplace(id, priority);
    if (!ok) {
      LOG(FATAL) << "could not emplace id: " << id << " into id_to_priority";
    }
    auto [__, ok2] = priority_id_set.emplace(PriorityId{priority, id});
    if (!ok2) {
      LOG(FATAL) << "could not emplace id: " << id << " into priority_id_set";
    }
  }

  size_t size() { return id_to_priority.size(); }

private:
  std::unordered_map<RPCID, int64_t> id_to_priority{};
  std::set<PriorityId> priority_id_set{};
};

class PPMQueue {
public:
  PPMQueue(std::unordered_map<std::string, RoutingEntry, TransparentHash,
                              TransparentEqual>
               routing);
  void push(const std::string &, RPCID, int64_t /*priority*/);
  RPCID pop(const std::string &, RPCID);
  RPCID pop(const std::string &);
  // checks if the service exists and the queue is not empty
  const std::string &check(std::string_view &);
  size_t size(const std::string &);

private:
  std::unordered_map<std::string, FlexiblePriorityQueue, TransparentHash,
                     TransparentEqual>
      ppm_queue{};
};
