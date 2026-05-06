#include "rpc_mapper.h"
#include "connection_enums.h"
#include "glog/logging.h"
#include "rpc_message.h"
#include <cstddef>
#include <cstdint>
#include <memory>

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
      id_map(std::unordered_map<
             ConnectionType,
             std::unordered_map<RPCID, std::shared_ptr<RPCMessage>>>()),
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
  id_map.emplace(ConnectionType::INGRESS,
                 std::unordered_map<RPCID, std::shared_ptr<RPCMessage>>());
  id_map.emplace(ConnectionType::EGRESS,
                 std::unordered_map<RPCID, std::shared_ptr<RPCMessage>>());
}

void RPCMapper::allocate_rpc(ConnectionType type, int32_t stream_id, int fd,
                             HTTP http) {
  auto rpc = pool.get_rpc(stream_id, fd, http, type);
  if (ds_map.at(type).find(fd) == ds_map.at(type).end()) {
    ds_map.at(type).emplace(
        fd, std::unordered_map<int32_t, std::shared_ptr<RPCMessage>>());
  }
  if (ds_map.at(type).at(fd).find(stream_id) == ds_map.at(type).at(fd).end()) {
    ds_map.at(type).at(fd).emplace(stream_id, std::move(rpc));
  } else {
    LOG(FATAL) << "RPC already exists for stream id: " << stream_id
               << " and fd: " << fd << " of type: " << type_to_str(type);
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
  auto rpc = ds_map.at(type).at(ds_fd).at(ds_stream_id);
  VLOG(1) << "RPCMapper: insering id: " << rpc->get_local_id()
          << " into id_map";
  auto [_, check] = id_map.at(type).emplace(rpc->get_local_id(), rpc);
  if (!check) {
    LOG(FATAL) << "id: " << rpc->get_local_id() << " already existed in id_map";
  }
  us_map.at(type).at(us_fd).emplace(us_stream_id, std::move(rpc));
  VLOG(1) << "Mapping DS stream id: " << ds_stream_id << " and fd: " << ds_fd
          << " to US stream id: " << us_stream_id << " and fd: " << us_fd;
}

std::shared_ptr<RPCMessage> &RPCMapper::get_ingress_rpc(RPCID id) {
  RPCID ingress_side_id = (uint64_t)id >> 32;
  if (ingress_side_id == 0) {
    LOG(FATAL) << "Upper 32 bits of local id is zero. This is not a valid "
                  "local id at the EGRESS side";
  }
  try {
    return id_map.at(ConnectionType::INGRESS).at(ingress_side_id);
  } catch (const std::out_of_range &e) {
    LOG(FATAL) << "No INGRESS RPC found for id: " << ingress_side_id;
  }
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
  id_map.at(type).erase(rpc->get_local_id());
  VLOG(1) << "RPCMapper: erased id: " << rpc->get_local_id() << " from id_map";
  pool.free_rpc(std::move(rpc));
}

size_t RPCMapper::get_fd_concurrency(int fd, ConnectionType type, bool is_us) {
  if (is_us) {
    return us_map.at(type).at(fd).size();
  } else {
    return ds_map.at(type).at(fd).size();
  }
}

bool RPCMapper::check_fd_exists(ConnectionType type, int fd, bool is_us) {
  try {
    if (is_us) {
      auto it = us_map.at(type).find(fd);
      if (it != us_map.at(type).end() && it->second.size() > 0) {
        return true;
      } else {
        return false;
      }
    } else {
      auto it = ds_map.at(type).find(fd);
      if (it != ds_map.at(type).end() && it->second.size() > 0) {
        return true;
      } else {
        return false;
      }
    }
  } catch (const std::out_of_range &e) {
    LOG(FATAL) << "Out of range exception in checking if FD exists: "
               << e.what() << " type: " << type_to_str(type) << " fd: " << fd
               << " is_us: " << is_us;
  } catch (const std::exception &e) {
    LOG(FATAL) << "Exception in checking if FD exists: " << e.what()
               << " type: " << type_to_str(type) << " fd: " << fd
               << " is_us: " << is_us;
  }
}