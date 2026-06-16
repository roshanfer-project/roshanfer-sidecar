#pragma once

#include <cstddef>
#include <string_view>

using ReplicaIndex = int;

struct TransparentHash {
  using is_transparent = void; // important for heterogeneous lookup
  std::size_t operator()(std::string_view txt) const noexcept {
    return std::hash<std::string_view>{}(txt);
  }
};

struct TransparentEqual {
  using is_transparent = void;
  bool operator()(std::string_view lhs, std::string_view rhs) const noexcept {
    return lhs == rhs;
  }
};