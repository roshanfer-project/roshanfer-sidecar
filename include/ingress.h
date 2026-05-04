#pragma once

#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>

class Ingress {
public:
  Ingress(int, std::string &, Stats &);
  ~Ingress();

  void enqueue(std::shared_ptr<RPCMessage>);
  std::optional<std::shared_ptr<RPCMessage>> dequeue(RPCQueue &, RPCMapper &);
  size_t size();
  void dump_state();
  bool send_dn_checker();
  RPCID get_tail_id();
  Priority get_tail_priority();

private:
  void drop_rpc(std::shared_ptr<RPCMessage>, RPCQueue &, RPCMapper &);
  void add_rpc_id_header(std::shared_ptr<RPCMessage> &);
  void add_priority_header(std::shared_ptr<RPCMessage> &);

  Stats &stats;
  std::deque<std::shared_ptr<RPCMessage>> queue;
  bool has_dn_on_fly;
  int32_t priority;
  int32_t drop_id;
  int drop_fd;
  RPCID last_rpc_id;
  const uint8_t *RPC_ID_HEADER_NAME =
      reinterpret_cast<const uint8_t *>("rpc-id");
  const uint8_t *PRIORITY_HEADER_NAME =
      reinterpret_cast<const uint8_t *>("priority");
  const size_t RPC_ID_HEADER_NAME_LEN = 7;
  const size_t PRIORITY_HEADER_NAME_LEN = 9;
  std::array<char, 32> rpc_id_header_value;
  std::array<char, 32> priority_header_value;
  std::string &ingress_service;
};