#pragma once

#include "config.h"
#include "rpc_message.h"
#include "utils.h"
#include <cstdint>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unordered_map>

class PPMQueue {
public:
  PPMQueue(std::unordered_map<std::string, RoutingEntry, TransparentHash,
                              TransparentEqual>
               routing);
  void push(std::shared_ptr<RPCMessage> rpc);
  std::shared_ptr<RPCMessage> pop(const std::string &service, int16_t id);
  // checks if the service exists and the queue is not empty
  const std::string &check(std::string_view &);
  size_t size(const std::string &);

private:
  std::unordered_map<std::string,
                     std::unordered_map<int16_t, std::shared_ptr<RPCMessage>>,
                     TransparentHash, TransparentEqual>
      ppm_queue;
};