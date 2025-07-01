#pragma once

#include <string>

enum class ConnectionType {
    INGRESS,
    EGRESS,
};

enum class ConnectionDirection {
    UPSTREAM,
    DOWNSTREAM,
};

enum class ConnectionStatus {
    UP, DOWN, TEARDOWN
};

enum class HTTP {
    HTTP1,
    HTTP2
};

std::string type_to_str(ConnectionType);
std::string direction_to_str(ConnectionDirection);