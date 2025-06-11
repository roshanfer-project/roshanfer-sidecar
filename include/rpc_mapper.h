#pragma once

#include <cstdint>
#include <unordered_map>
#include "connection_enums.h"
#include "rpc_message.h"


const size_t MAX_gRPC_POOL_SIZE = 100;
const size_t MAX_HTTP_POOL_SIZE = 1000;

class RPCMapper {
    public:
        RPCMapper();
        void allocate_rpc(ConnectionType, uint32_t, int, bool);
        void route(ConnectionType, uint32_t, int, uint32_t, int);
        RPCMessage* get_us_rpc(ConnectionType, uint32_t, int);
        RPCMessage* get_ds_rpc(ConnectionType, uint32_t, int);
        void remove_rpc(ConnectionType, RPCMessage*&);
    
    private:
    std::unordered_map<ConnectionType,
    std::unordered_map<int, std::unordered_map<uint32_t,
     RPCMessage*>>> ds_map;
    std::unordered_map<ConnectionType,
        std::unordered_map<int, std::unordered_map<uint32_t,
        RPCMessage*>>> us_map;
    RPCMessagePool pool;

};