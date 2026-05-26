#pragma once

#include <cstdint>
#include <string>

enum class ConnectionType : uint8_t {
  INGRESS,
  EGRESS,
};

enum class ConnectionDirection : uint8_t {
  UPSTREAM,
  DOWNSTREAM,
};

enum class ConnectionStatus : uint8_t { UP, DOWN, TEARDOWN };

enum class HTTP : uint8_t { HTTP1, HTTP2 };

std::string type_to_str(ConnectionType);
std::string direction_to_str(ConnectionDirection);