#pragma once

#include "connection_enums.h"
#include <unordered_map>

class Stats {
    public:
        Stats();

    public:
        std::unordered_map<ConnectionType, int> sidecar_resp_in;
        bool new_response_in;
};