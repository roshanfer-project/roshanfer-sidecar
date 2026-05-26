#pragma once

#include "connection_enums.hpp"
#include "rpc_message.hpp"
#include <cstdint>
#include <memory>
#include <unordered_map>

const size_t MAX_gRPC_POOL_SIZE = 4000;
const size_t MAX_HTTP_POOL_SIZE = 10000;

class RPCMapper {
public:
  RPCMapper();
  void allocate_rpc(ConnectionType, int32_t, int, HTTP);
  void route(ConnectionType, int32_t, int, int32_t, int);
  std::shared_ptr<RPCMessage> get_us_rpc(ConnectionType, int32_t, int);
  std::shared_ptr<RPCMessage> get_ds_rpc(ConnectionType, int32_t, int);
  void remove_rpc(ConnectionType, std::shared_ptr<RPCMessage>);
  bool check_fd_exists(ConnectionType, int, bool);
  size_t get_fd_concurrency(int, ConnectionType, bool);
  std::shared_ptr<RPCMessage> &get_ingress_rpc(RPCID);

private:
  std::unordered_map<
      ConnectionType,
      std::unordered_map<
          int, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>>>
      ds_map;
  std::unordered_map<
      ConnectionType,
      std::unordered_map<
          int, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>>>
      us_map;
  std::unordered_map<ConnectionType,
                     std::unordered_map<RPCID, std::shared_ptr<RPCMessage>>>
      id_map;
  RPCMessagePool pool;
};