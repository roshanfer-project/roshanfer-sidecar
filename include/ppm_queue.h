#pragma once

#include "config.h"
#include "rpc_message.h"
#include "utils.h"
#include <queue>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unordered_map>

class PPMQueue {
public:
  PPMQueue(std::unordered_map<std::string, RoutingEntry, TransparentHash,
                              TransparentEqual>
               routing);
  void enqueue(std::shared_ptr<RPCMessage> rpc);
  std::shared_ptr<RPCMessage> dequeue(const std::string& service);
  // checks if the service exists and the queue is not empty
  const std::string &check(std::string_view &);
  int get_fd(const std::string &);
  size_t size(const std::string &);
  int queueing_delay(const std::string &);

private:
  std::unordered_map<std::string, std::queue<std::shared_ptr<RPCMessage>>,
                     TransparentHash, TransparentEqual>
      ppm_queue;
};