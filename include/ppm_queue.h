#pragma once

#include "config.h"
#include "rpc_message.h"
#include "utils.h"
#include <cstddef>
#include <cstdint>
#include <map>
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

private:
  std::unordered_map<std::string, std::map<RPCID, std::shared_ptr<RPCMessage>>,
                     TransparentHash, TransparentEqual>
      ppm_queue;
};
