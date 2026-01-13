#pragma once

#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <random>
#include <string>

class Ingress {
public:
  Ingress(int, std::string &);
  ~Ingress();

  void enqueue(std::shared_ptr<RPCMessage> rpc);
  std::shared_ptr<RPCMessage> dequeue();
  size_t size();
  int64_t add_to_be_admitted_or_drop(RPCQueue &, RPCMapper &, int32_t);
  void dump_state();
  void add_rpc_id_header(std::shared_ptr<RPCMessage> &);
  void add_priority_header(std::shared_ptr<RPCMessage> &);

private:
  std::deque<std::shared_ptr<RPCMessage>> queue;
  int32_t priority;
  int32_t drop_id;
  int drop_fd;
  float max_th_us;
  float min_th_us;
  int32_t red_count;
  std::random_device rd;
  std::mt19937 gen;
  std::uniform_real_distribution<> dis;
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