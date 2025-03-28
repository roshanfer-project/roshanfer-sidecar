#include "stats.h"

Stats::Stats() 
    :   sidecar_resp_in()
    {
        sidecar_resp_in[ConnectionType::INGRESS] = 0;
        sidecar_resp_in[ConnectionType::EGRESS] = 0;
    }