#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include "connection_enums.h"
#include "rpc_message.h"


class RPCMapper {
    public:
        RPCMapper();
        void allocate_rpc(ConnectionType, uint32_t, int);
        void route(ConnectionType, uint32_t, uint32_t);
        std::shared_ptr<RPCMessage>& get_us_rpc(ConnectionType, uint32_t);
        std::shared_ptr<RPCMessage>& get_ds_rpc(ConnectionType, uint32_t);
        void remove_rpc(ConnectionType, uint32_t);
    
    private:
        std::unordered_map<ConnectionType,
            std::unordered_map<uint32_t, std::shared_ptr<RPCMessage>>> ds_map;
        std::unordered_map<ConnectionType,
            std::unordered_map<uint32_t, std::shared_ptr<RPCMessage>>> us_map;

};