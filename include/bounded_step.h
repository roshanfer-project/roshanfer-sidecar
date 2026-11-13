#pragma once

#include <algorithm>
#include <cstdint>

// Moves a limit toward target by at most half of prev per update. The upper
// bound rounds up, so a limit of 1 can reach 2.
inline int32_t bounded_step(int32_t prev, int32_t target) {
  return std::clamp(target, prev - prev / 2, prev + (prev + 1) / 2);
}
