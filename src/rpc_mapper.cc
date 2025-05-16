#include "rpc_mapper.h"
#include "connection_enums.h"
#include <cstdint>
#include "glog/logging.h"

RPCMapper::RPCMapper()
    :   ds_map(std::unordered_map<ConnectionType,
            std::unordered_map<int, std::unordered_map<uint32_t,
            RPCMessage*>>>()),
        us_map(std::unordered_map<ConnectionType,
            std::unordered_map<int, std::unordered_map<uint32_t,
            RPCMessage*>>>()),
        pool(RPCMessagePool(MAX_POOL_SIZE))
{
    ds_map.emplace(ConnectionType::INGRESS, std::unordered_map<int, std::unordered_map<uint32_t,
        RPCMessage*>>());
    ds_map.emplace(ConnectionType::EGRESS, std::unordered_map<int, std::unordered_map<uint32_t,
        RPCMessage*>>());
    us_map.emplace(ConnectionType::EGRESS, std::unordered_map<int, std::unordered_map<uint32_t,
        RPCMessage*>>());
    us_map.emplace(ConnectionType::INGRESS, std::unordered_map<int, std::unordered_map<uint32_t,
        RPCMessage*>>());
}

void RPCMapper::allocate_rpc(ConnectionType type, uint32_t stream_id, int fd) {
    auto rpc = pool.get_rpc(stream_id, fd);
    if (ds_map[type].find(fd) == ds_map[type].end()) {
        ds_map[type].emplace(fd, std::unordered_map<uint32_t, RPCMessage*>());
    }
    ds_map[type][fd].emplace(stream_id, rpc);
}

/*
    This methods creates a mapping between downstream and upstream RPC identifiers. It is mostly used after
    the routing of a RPC request. After this, the user can find the RPC using upstream identifiers whenerver
    the response has been received.
*/
void RPCMapper::route(ConnectionType type, uint32_t ds_stream_id, int ds_fd, uint32_t us_stream_id, int us_fd) {
    if (us_map[type].find(us_fd) == us_map[type].end()) {
        us_map[type].emplace(us_fd, std::unordered_map<uint32_t, RPCMessage*>());
    }
    us_map[type][us_fd].emplace(us_stream_id, ds_map[type].at(ds_fd).at(ds_stream_id));
    VLOG(1) << "Mapping DS stream id: " << ds_stream_id << " and fd: " << ds_fd
            << " to US stream id: " << us_stream_id << " and fd: " << us_fd;
}

RPCMessage* RPCMapper::get_us_rpc(ConnectionType type, uint32_t us_stream_id, int us_fd) {
    try {
        auto& rpc = us_map[type][us_fd].at(us_stream_id);
        VLOG(2) << "Access rpc with get_us_rpc. ds_fd: " << rpc->ds_fd
                << " ds_stream_id: " << rpc->ds_stream_id
                << " us_fd: " << us_fd
                << " us_stream_id: " << us_stream_id; 
        return rpc;
    } catch (const std::out_of_range& e) {
        LOG(FATAL) << "No US RPC found for fd: " << us_fd << " and stream id: " 
                    << us_stream_id << " of type: " << type_to_str(type);
    }
}

RPCMessage* RPCMapper::get_ds_rpc(ConnectionType type, uint32_t ds_stream_id, int ds_fd) {
    try {
        auto& rpc = ds_map[type][ds_fd].at(ds_stream_id);
        VLOG(2) << "Access rpc with get_ds_rpc. ds_fd: " << ds_fd
                << " ds_stream_id: " << ds_stream_id
                << " us_fd: " << rpc->us_fd
                << " us_stream_id: " << rpc->us_stream_id;
        return rpc;
    } catch (const std::out_of_range& e) {
        LOG(FATAL) << "No DS RPC found for fd: " << ds_fd << " and stream id: " << ds_stream_id
                    << " of type: " << type_to_str(type);
    }
}

void RPCMapper::remove_rpc(ConnectionType type, RPCMessage*& rpc) {
    us_map[type][rpc->us_fd].erase(rpc->us_stream_id);
    ds_map[type][rpc->ds_fd].erase(rpc->ds_stream_id);
    pool.free_rpc(rpc);
    rpc = nullptr;
}



