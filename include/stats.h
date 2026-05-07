#pragma once

#include "fast_map.hpp"
#include "hdr/hdr_histogram.h"
#include "quantile_estimator.hpp"
#include "rpc_message.h"
#include "tdigest.h"
#include <chrono>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#ifdef NANO_LOG_ENABLED
#include "config.h"
#endif

class Utilization {
public:
  Utilization(uint32_t, std::vector<std::string> services);
  void update(uint32_t, std::string &);

private:
  void report(std::string &service);

private:
  LocalMap<double> total;
  LocalMap<uint32_t> last_in;
  LocalMap<uint32_t> count;
  uint32_t period;
  LocalMap<std::chrono::steady_clock::time_point> last_update;
  LocalMap<std::chrono::steady_clock::time_point> last_report;
  struct hdr_histogram *hist;
};

class MovingAverage {
public:
  MovingAverage();

  // delete copy semantics
  MovingAverage(const MovingAverage &) = delete;
  MovingAverage &operator=(const MovingAverage &) = delete;

  // delete move semantics
  MovingAverage(MovingAverage &&) = delete;
  MovingAverage &operator=(MovingAverage &&) = delete;

  void update(int32_t);
  float get_value();
  uint32_t get_count() const;
  void set_description(std::string desc) { description = desc; }

private:
  uint32_t count;
  float value;
  std::string description;
};

class ExponentialMovingAverage {
public:
  ExponentialMovingAverage();
  ExponentialMovingAverage(float);

  // delete copy semantics
  ExponentialMovingAverage(const ExponentialMovingAverage &) = delete;
  ExponentialMovingAverage &
  operator=(const ExponentialMovingAverage &) = delete;

  // delete move semantics
  ExponentialMovingAverage(ExponentialMovingAverage &&) = delete;
  ExponentialMovingAverage &operator=(ExponentialMovingAverage &&) = delete;

  void update(int32_t);
  float get_value();
  float get_value_cap(float, float);
  uint32_t get_count() const;
  void set_description(std::string desc) { description = desc; }
  void set_alpha(float new_alpha) { this->alpha = new_alpha; }
  void up();
  void down();

private:
  uint32_t count;
  float value;
  std::string description;
  float alpha;
  int32_t last_value;
};

class Counter {
public:
  Counter(std::string);

  void up(int32_t);
  void down(int32_t);
  int32_t get_count();

private:
  int32_t count;
  std::string description;
};

class TDigest {
public:
  TDigest() : td(td_new(200)), count(0), description("") {}

  void add(int32_t);
  double get_quantile(double q) {
    double val = td_value_at(td, q);
    return std::isnan(val) ? 0 : val;
  }
  void set_description(std::string dec) { description = dec; }

  // delete copy semantics
  TDigest(const TDigest &) = delete;
  TDigest &operator=(const TDigest &) = delete;

  // delete move semantics
  TDigest(TDigest &&) = delete;
  TDigest &operator=(TDigest &&) = delete;

private:
  td_histogram_t *td;
  int64_t count;
  std::string description;
};

// O(1) state: ∫ x dt since last flush in area; last_flush starts a window.
// update uses steady_clock::now(); value(now) uses the same clock unless
// overridden. Piecewise-constant x between updates.
class TimeWeightedMean {
public:
  using Clock = std::chrono::steady_clock;
  static constexpr std::chrono::milliseconds kNanologSnapshotPeriod{250};

  TimeWeightedMean() : area_(0.0), last_x_(0.0) {}

  void up() { up(Clock::now()); }

  void up(Clock::time_point t) {
    const double b = last_flush_.has_value() ? last_x_ : 0.0;
    update(b + 1.0, t);
  }

  void down() { down(Clock::now()); }

  void down(Clock::time_point t) {
    const double b = last_flush_.has_value() ? last_x_ : 0.0;
    if (b - 1 < 0) {
      LOG(FATAL) << "negative value in down method of TimeWeightedMean with "
                    "description: "
                 << description;
    }
    update(b - 1.0, t);
  }

  void update(double sample) { update(sample, Clock::now()); }

  void update(double sample, Clock::time_point t) {
    if (!last_flush_.has_value()) {
      last_flush_ = t;
      last_t_ = t;
      last_x_ = sample;
      return;
    }
    const double dt = std::chrono::duration<double>(t - *last_t_).count();
    if (dt > 0.0)
      area_ += last_x_ * dt;
    last_x_ = sample;
    last_t_ = t;
#ifdef NANO_LOG_ENABLED
    if (!description.empty() && last_value_call_time_.has_value() &&
        (t - *last_value_call_time_) >= kNanologSnapshotPeriod &&
        (!last_snapshot_nanolog_time_.has_value() ||
         (t - *last_snapshot_nanolog_time_) >= kNanologSnapshotPeriod)) {
      NANO_LOG(NOTICE, "M# %s TwAvg %s N:N %f", config.name.c_str(),
               description.c_str(), snapshot_mean(t));
      last_snapshot_nanolog_time_ = t;
    }
#endif
  }

  double value() { return value(Clock::now()); }

  double value(Clock::time_point now) {
    if (!last_flush_.has_value() || !(now > *last_flush_))
      return 0;
    if (now > *last_t_)
      area_ += last_x_ * std::chrono::duration<double>(now - *last_t_).count();
    area_ /= std::chrono::duration<double>(now - *last_flush_).count();
    last_flush_ = now;
    last_t_ = now;
#ifdef NANO_LOG_ENABLED
    if (description == "") {
      LOG(FATAL) << "Description of TwAvg is empty";
    }
    NANO_LOG(NOTICE, "M# %s TwAvg %s N:N %f", config.name.c_str(),
             description.c_str(), area_);
    last_value_call_time_ = now;
    last_snapshot_nanolog_time_ = now;
#endif
    return std::exchange(area_, 0.0);
  }

  void set_description(std::string desc) { description = desc; }

  bool empty() const { return !last_flush_.has_value(); }

private:
#ifdef NANO_LOG_ENABLED
  double snapshot_mean(Clock::time_point now) const {
    if (!last_flush_.has_value() || !(now > *last_flush_))
      return 0.0;
    double snap = area_;
    if (now > *last_t_)
      snap += last_x_ * std::chrono::duration<double>(now - *last_t_).count();
    const double span =
        std::chrono::duration<double>(now - *last_flush_).count();
    if (span <= 0.0)
      return 0.0;
    return snap / span;
  }
#endif

  std::optional<Clock::time_point> last_flush_;
  std::optional<Clock::time_point> last_t_;
  double area_;
  double last_x_;
  std::string description;
#ifdef NANO_LOG_ENABLED
  std::optional<Clock::time_point> last_value_call_time_;
  std::optional<Clock::time_point> last_snapshot_nanolog_time_;
#endif
};

class Stats {
public:
  Stats(std::vector<std::string>, std::vector<std::string>);

  // delete copy semantics
  Stats(const Stats &) = delete;
  Stats &operator=(const Stats &) = delete;

  // delete move semantics
  Stats(Stats &&) = delete;
  Stats &operator=(Stats &&) = delete;

  void report_latency(const std::shared_ptr<RPCMessage> &rpc);

public:
  Counter mode2_credits;
  LocalMap<ExponentialMovingAverage> ema_ds_service_time_us;
  LocalMap<ExponentialMovingAverage> ema_us_service_time_us;
  LocalMap<ExponentialMovingAverage> ema_us_sidecar_rtt_us;
  LocalMap<ExponentialMovingAverage> ema_ds_sidecar_rtt_us;
  LocalMap<SmoothedQuantileEstimator> tail_ds_service_time_us;
  LocalMap<SmoothedQuantileEstimator> tail_e2e_time_us;
  LocalMap<TimeWeightedMean> time_mean_ds_concurrency;
  LocalMap<ExponentialMovingAverage> ema_wait_to_tx_us;
};