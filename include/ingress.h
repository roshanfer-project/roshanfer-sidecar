#pragma once

#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>

class Ingress {
public:
  Ingress(int, std::string &);
  ~Ingress();

  void enqueue(std::shared_ptr<RPCMessage> rpc);
  std::shared_ptr<RPCMessage> dequeue();
  size_t size();
  void update_stats(int32_t, int32_t);
  int64_t add_to_be_admitted_or_drop(RPCQueue &, RPCMapper &, int32_t);
  void dump_state();
  void add_rpc_id_header(std::shared_ptr<RPCMessage> &);

public:
  int32_t p95_us;
  int32_t p50_us;

private:
  std::deque<std::shared_ptr<RPCMessage>> queue;
  int32_t drop_id;
  int drop_fd;
  int32_t slo_us;
  int32_t last_rpc_id;
  const uint8_t *RPC_ID_HEADER_NAME =
      reinterpret_cast<const uint8_t *>("rpc-id");
  const size_t RPC_ID_HEADER_NAME_LEN = 7;
  std::array<char, 20> rpc_id_header_value;
  std::string &ingress_service;
};