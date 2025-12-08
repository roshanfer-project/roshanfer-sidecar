#pragma once

#include "config.h"
#include "fast_map.hpp"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

class Ingress {
public:
  Ingress(std::unordered_map<std::string, RoutingEntry, TransparentHash,
                             TransparentEqual>,
          int);
  ~Ingress();

  void enqueue(std::shared_ptr<RPCMessage> rpc);
  std::shared_ptr<RPCMessage> dequeue(std::string);
  size_t size(std::string);
  void update_stats(int32_t, int32_t, std::string &);
  int64_t add_to_be_admitted_or_drop(RPCQueue &, RPCMapper &, std::string &,
                                     int64_t, int32_t, int32_t);
  void dump_state();
  void add_rpc_id_header(std::shared_ptr<RPCMessage> &);

public:
  LocalMap<int32_t> p95_us;
  LocalMap<int32_t> p50_us;

private:
  std::unordered_map<std::string, std::deque<std::shared_ptr<RPCMessage>>>
      queue;
  LocalMap<int32_t> drop_id;
  std::vector<std::string> services;
  int drop_fd;
  LocalMap<int32_t> slo_us;
  int32_t last_rpc_id;
  const uint8_t *RPC_ID_HEADER_NAME =
      reinterpret_cast<const uint8_t *>("rpc-id");
  const size_t RPC_ID_HEADER_NAME_LEN = 7;
  std::array<char, 20> rpc_id_header_value;
};