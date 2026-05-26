#pragma once

#include <array>
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>

inline std::string errno_string(int err) {
  std::array<char, 256> buf{};
  strerror_r(err, buf.data(), buf.size());
  return {buf.data()};
}


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