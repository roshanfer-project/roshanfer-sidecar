#pragma once

#include "connection_enums.h"
#include "rpc_message.h"
#include <cstdint>
#include <unordered_map>

const size_t MAX_gRPC_POOL_SIZE = 200;
const size_t MAX_HTTP_POOL_SIZE = 1000;

class RPCMapper {
public:
  RPCMapper();
  void allocate_rpc(ConnectionType, int32_t, int, HTTP);
  void route(ConnectionType, int32_t, int, int32_t, int);
  std::shared_ptr<RPCMessage> get_us_rpc(ConnectionType, int32_t, int);
  std::shared_ptr<RPCMessage> get_ds_rpc(ConnectionType, int32_t, int);
  void remove_rpc(ConnectionType, std::shared_ptr<RPCMessage>);
  bool check_fd_exists(ConnectionType, int, bool);

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
  RPCMessagePool pool;
};