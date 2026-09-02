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
#include <tuple>

class Ingress {
public:
  Ingress(int, std::string &, Stats &, RPCMapper &, RPCQueue &);
  ~Ingress();

  void enqueue(std::shared_ptr<RPCMessage>);
  std::optional<std::shared_ptr<RPCMessage>> dequeue();
  size_t size();
  const std::string &expected_service() const { return ingress_service; }
  void update_ingress_cap();
  void dump_state();
  std::optional<std::tuple<RPCID, Priority>> send_credit_request_checker();

private:
  void drop_rpc(std::shared_ptr<RPCMessage>);
  void add_rpc_id_header(std::shared_ptr<RPCMessage> &);
  void add_priority_header(std::shared_ptr<RPCMessage> &);
  void add_deadline_header(std::shared_ptr<RPCMessage> &);

  // AIMD members
  size_t ingress_size_cap = 10;
  const float aimd_err_d = -0.05F;
  const float aimd_err_i = -0.3F;
  const float aimd_adj_d = 1.5F;
  const float aimd_adj_i = 1.0F;
  const float safe_multiply = 2.0F;
  const float aimd_queue_th = 0.8F;

  Stats &stats;
  RPCMapper &rpc_mapper;
  RPCQueue &rpc_queue;
  std::deque<std::shared_ptr<RPCMessage>> queue;
  TimeWeightedMean ingress_mean;
  size_t credit_requests_on_fly = 0;
  int32_t priority;
  int32_t drop_id;
  int drop_fd;
  RPCID last_rpc_id;
  const uint8_t *RPC_ID_HEADER_NAME =
      reinterpret_cast<const uint8_t *>("rpc-id");
  const uint8_t *PRIORITY_HEADER_NAME =
      reinterpret_cast<const uint8_t *>("priority");
  const uint8_t *DEADLINE_HEADER_NAME =
      reinterpret_cast<const uint8_t *>("deadline");
  const size_t RPC_ID_HEADER_NAME_LEN = 7;
  const size_t PRIORITY_HEADER_NAME_LEN = 9;
  const size_t DEADLINE_HEADER_NAME_LEN = 9;
  std::array<char, 32> rpc_id_header_value;
  std::array<char, 32> priority_header_value;
  std::array<char, 32> deadline_header_value;
  std::string &ingress_service;
};