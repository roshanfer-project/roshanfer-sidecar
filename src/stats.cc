#include "stats.h"

Stats::Stats(std::vector<RoutingEntry> routing) 
    :   sidecar_resp_in(),
        new_response_in(std::unordered_map<std::string, bool>())
    {
        sidecar_resp_in[ConnectionType::INGRESS] = 0;
        sidecar_resp_in[ConnectionType::EGRESS] = 0;
        for (auto& route : routing) {
            new_response_in.emplace(route.service, false);
        }
    }