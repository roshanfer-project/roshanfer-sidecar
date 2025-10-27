#include "rpc_mapper.h"
#include "connection_enums.h"
#include "glog/logging.h"
#include "rpc_message.h"
#include <cstdint>

RPCMapper::RPCMapper()
    : ds_map(std::unordered_map<
             ConnectionType,
             std::unordered_map<
                 int,
                 std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>>>()),
      us_map(std::unordered_map<
             ConnectionType,
             std::unordered_map<
                 int,
                 std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>>>()),
      pool(RPCMessagePool(MAX_gRPC_POOL_SIZE, MAX_HTTP_POOL_SIZE)) {
  ds_map.emplace(
      ConnectionType::INGRESS,
      std::unordered_map<
          int, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>>());
  ds_map.emplace(
      ConnectionType::EGRESS,
      std::unordered_map<
          int, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>>());
  us_map.emplace(
      ConnectionType::EGRESS,
      std::unordered_map<
          int, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>>());
  us_map.emplace(
      ConnectionType::INGRESS,
      std::unordered_map<
          int, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>>());
}

void RPCMapper::allocate_rpc(ConnectionType type, int32_t stream_id, int fd,
                             HTTP http) {
  auto rpc = pool.get_rpc(stream_id, fd, http);
  if (ds_map.at(type).find(fd) == ds_map.at(type).end()) {
    ds_map.at(type).emplace(
        fd, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>());
  }
  if (ds_map.at(type).at(fd).find(stream_id) == ds_map.at(type).at(fd).end()) {
    ds_map.at(type).at(fd).emplace(stream_id, std::move(rpc));
  } else {
    LOG(FATAL) << "RPC already exists for stream id: " << stream_id
               << " and fd: " << fd
               << " of type: " << type_to_str(type);
  }
}

/*
    This methods creates a mapping between downstream and upstream RPC
   identifiers. It is mostly used after the routing of a RPC request. After
   this, the user can find the RPC using upstream identifiers whenerver the
   response has been received.
*/
void RPCMapper::route(ConnectionType type, int32_t ds_stream_id, int ds_fd,
                      int32_t us_stream_id, int us_fd) {
  if (us_map.at(type).find(us_fd) == us_map.at(type).end()) {
    us_map[type].emplace(
        us_fd, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>());
  }
  us_map.at(type).at(us_fd).emplace(us_stream_id,
                                    ds_map.at(type).at(ds_fd).at(ds_stream_id));
  VLOG(1) << "Mapping DS stream id: " << ds_stream_id << " and fd: " << ds_fd
          << " to US stream id: " << us_stream_id << " and fd: " << us_fd;
}

std::shared_ptr<RPCMessage>
RPCMapper::get_us_rpc(ConnectionType type, int32_t us_stream_id, int us_fd) {
  try {
    auto &rpc = us_map.at(type).at(us_fd).at(us_stream_id);
    VLOG(2) << "Access rpc with get_us_rpc. ds_fd: " << rpc->get_ds_fd()
            << " ds_stream_id: " << rpc->get_ds_stream_id()
            << " us_fd: " << us_fd << " us_stream_id: " << us_stream_id;
    return rpc;
  } catch (const std::out_of_range &e) {
    LOG(FATAL) << "No US RPC found for fd: " << us_fd
               << " and stream id: " << us_stream_id
               << " of type: " << type_to_str(type);
  }
}

std::shared_ptr<RPCMessage>
RPCMapper::get_ds_rpc(ConnectionType type, int32_t ds_stream_id, int ds_fd) {
  try {
    auto &rpc = ds_map.at(type).at(ds_fd).at(ds_stream_id);
    VLOG(2) << "Access rpc with get_ds_rpc. ds_fd: " << ds_fd
            << " ds_stream_id: " << ds_stream_id
            << " us_fd: " << rpc->get_us_fd()
            << " us_stream_id: " << rpc->get_us_stream_id();
    return rpc;
  } catch (const std::out_of_range &e) {
    LOG(FATAL) << "No DS RPC found for fd: " << ds_fd
               << " and stream id: " << ds_stream_id
               << " of type: " << type_to_str(type);
  }
}

void RPCMapper::remove_rpc(ConnectionType type,
                           std::shared_ptr<RPCMessage> rpc) {
  us_map.at(type).at(rpc->get_us_fd()).erase(rpc->get_us_stream_id());
  ds_map.at(type).at(rpc->get_ds_fd()).erase(rpc->get_ds_stream_id());
  pool.free_rpc(std::move(rpc));
}