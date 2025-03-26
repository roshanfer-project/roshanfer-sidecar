#include "rpc_mapper.h"
#include "connection_enums.h"
#include <cstdint>

RPCMapper::RPCMapper() 
    : ds_map(std::unordered_map<ConnectionType,
        std::unordered_map<uint32_t, std::shared_ptr<RPCMessage>>>()),
      us_map(std::unordered_map<ConnectionType,
        std::unordered_map<uint32_t, std::shared_ptr<RPCMessage>>>())
{
    ds_map.emplace(ConnectionType::INGRESS, std::unordered_map<uint32_t, std::shared_ptr<RPCMessage>>());
    ds_map.emplace(ConnectionType::EGRESS, std::unordered_map<uint32_t, std::shared_ptr<RPCMessage>>());
    us_map.emplace(ConnectionType::INGRESS, std::unordered_map<uint32_t, std::shared_ptr<RPCMessage>>());
    us_map.emplace(ConnectionType::EGRESS, std::unordered_map<uint32_t, std::shared_ptr<RPCMessage>>());
}

void RPCMapper::allocate_rpc(ConnectionType type, uint32_t stream_id, int fd) {
    auto rpc = std::make_shared<RPCMessage>(stream_id, fd);
    ds_map[type].emplace(stream_id, rpc);
}

void RPCMapper::route(ConnectionType type, uint32_t ds_stream_id, uint32_t us_stream_id) {
    us_map[type].emplace(us_stream_id, ds_map[type].at(ds_stream_id));
}

std::shared_ptr<RPCMessage>& RPCMapper::get_us_rpc(ConnectionType type, uint32_t stream_id) {
    return us_map[type].at(stream_id);
}

std::shared_ptr<RPCMessage>& RPCMapper::get_ds_rpc(ConnectionType type, uint32_t stream_id) {
    return ds_map[type].at(stream_id);
}

void RPCMapper::remove_rpc(ConnectionType type, uint32_t ds_stream_id) {
    auto& rpc = ds_map[type].at(ds_stream_id);
    us_map[type].erase(rpc->us_stream_id);
    ds_map[type].erase(ds_stream_id);
}



