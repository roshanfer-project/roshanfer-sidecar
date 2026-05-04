#pragma once

#include "config.h"
#include "glog/logging.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <vector>

#if defined(NANO_LOG_ENABLED) || defined(NABO_LOG_TRACE_ENABLED)
#include "NanoLog.h"
#include "NanoLogCpp17.h"

using namespace NanoLog::LogLevels;
#endif
namespace smoothed_quantile_estimator_detail {

inline int max_quantile_sorted_index(int n, double q) {
  if (n <= 1)
    return std::max(0, n - 1);
  if (q <= 0)
    return 0;
  if (q >= 1)
    return n - 1;
  const double h = (n - 1) * q;
  const int lo = static_cast<int>(std::floor(h));
  const int hi = static_cast<int>(std::ceil(h));
  return std::max(lo, hi);
}

inline void partial_sort_for_quantile(std::vector<double> &v, double q) {
  const auto n = v.size();
  if (n <= 1)
    return;
  const int max_idx = max_quantile_sorted_index(static_cast<int>(n), q);
  const auto need = static_cast<size_t>(max_idx) + 1;
  if (need >= n)
    std::sort(v.begin(), v.end());
  else
    std::partial_sort(v.begin(), v.begin() + static_cast<std::ptrdiff_t>(need),
                      v.end());
}

inline double quantile_sorted(const std::vector<double> &s, double q) {
  if (s.empty())
    return std::numeric_limits<double>::quiet_NaN();
  int n = static_cast<int>(s.size());
  if (n == 1)
    return s[0];
  if (q <= 0)
    return s[0];
  if (q >= 1)
    return s[static_cast<size_t>(n - 1)];
  double h = (n - 1) * q;
  int lo = static_cast<int>(std::floor(h));
  int hi = static_cast<int>(std::ceil(h));
  if (lo >= hi)
    return s[static_cast<size_t>(lo)];
  return s[static_cast<size_t>(lo)] +
         (h - lo) * (s[static_cast<size_t>(hi)] - s[static_cast<size_t>(lo)]);
}

} // namespace smoothed_quantile_estimator_detail

// Batches samples; on each flush (every time_period or sample_period samples),
// computes empirical target_quantile, then EMA-smooths into value().
class SmoothedQuantileEstimator {
public:
  using Clock = std::chrono::steady_clock;

  SmoothedQuantileEstimator(
      double ema_alpha = 0.7,
      std::chrono::milliseconds time_period = std::chrono::milliseconds(200),
      std::size_t sample_period = 100, double target_quantile = 0.99)
      : ema_alpha_(ema_alpha), time_period_(time_period),
        sample_period_(sample_period), target_q_(target_quantile),
        smoothed_(std::numeric_limits<double>::quiet_NaN()) {
    const std::size_t cap = std::max(sample_period_, std::size_t{1});
    buf_.reserve(cap);
    scratch_.reserve(cap);
  }

  void update(double sample) { update(sample, Clock::now()); }

  void update(double sample, Clock::time_point now) {
    buf_.push_back(sample);
    if (!next_flush_.has_value())
      next_flush_ = now + time_period_;
    const bool by_n = buf_.size() >= sample_period_;
    const bool by_t = now >= *next_flush_;
    if (by_n || by_t)
      flush_at(now);
  }

  void flush_pending() { flush_pending(Clock::now()); }

  void flush_pending(Clock::time_point now) {
    if (!buf_.empty())
      flush_at(now);
  }

  double value() const { return has_value() ? smoothed_ : 0.0; }
  bool has_value() const { return std::isfinite(smoothed_); }

  void set_description(std::string desc) { description = desc; }

private:
  void flush_at(Clock::time_point now) {
    if (buf_.empty()) {
      next_flush_ = now + time_period_;
      return;
    }
    scratch_.resize(buf_.size());
    std::copy(buf_.begin(), buf_.end(), scratch_.begin());
    buf_.clear();
    smoothed_quantile_estimator_detail::partial_sort_for_quantile(scratch_,
                                                                  target_q_);
    const double q = smoothed_quantile_estimator_detail::quantile_sorted(
        scratch_, target_q_);
    scratch_.clear();
    next_flush_ = now + time_period_;
    if (!std::isfinite(q))
      return;
    if (!std::isfinite(smoothed_))
      smoothed_ = q;
    else
      smoothed_ = ema_alpha_ * q + (1.0 - ema_alpha_) * smoothed_;

#ifdef NANO_LOG_ENABLED
    if (description == "") {
      LOG(FATAL) << "emptry description in Qunatile Estimator";
    }

    NANO_LOG(NOTICE, "M# %s Tail-%f %s T:T %f", config.name.c_str(), target_q_,
             description.c_str(), q);
    NANO_LOG(NOTICE, "M# %s Tail-%f-Smooth %s T:T %f", config.name.c_str(),
             target_q_, description.c_str(), smoothed_);
#endif
  }

  std::string description;
  double ema_alpha_;
  std::chrono::milliseconds time_period_;
  std::size_t sample_period_;
  double target_q_;
  std::vector<double> buf_;
  std::vector<double> scratch_;
  std::optional<Clock::time_point> next_flush_;
  double smoothed_;
};
