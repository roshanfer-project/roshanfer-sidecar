#pragma once

#include "fast_map.hpp"
#include "quantile_estimator.hpp"
#include "rpc_message.h"
#include "tdigest.h"
#include <chrono>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#ifdef SIDECAR_METRICS
#include "metrics.h"
#endif

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
  void set_description(std::string desc) {
    description = desc;
#ifdef SIDECAR_METRICS
    metric_id = metrics::id("MA " + description + " T:T");
#endif
  }

private:
  uint32_t count;
  float value;
  std::string description;
#ifdef SIDECAR_METRICS
  uint16_t metric_id = 0;
#endif
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
  void set_description(std::string desc) {
    description = desc;
#ifdef SIDECAR_METRICS
    metric_id = metrics::id("EMA " + description + " T:T");
#endif
  }
  void set_alpha(float new_alpha) { this->alpha = new_alpha; }
  void up();
  void down();

private:
  uint32_t count;
  float value;
  std::string description;
  float alpha;
  int32_t last_value;
#ifdef SIDECAR_METRICS
  uint16_t metric_id = 0;
#endif
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
  void set_description(std::string dec) {
    description = dec;
#ifdef SIDECAR_METRICS
    p95_id = metrics::id("TD-95 " + description + " T:T");
    p99_id = metrics::id("TD-99 " + description + " T:T");
#endif
  }

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
#ifdef SIDECAR_METRICS
  uint16_t p95_id = 0;
  uint16_t p99_id = 0;
#endif
};

// O(1) state: ∫ x dt since last flush in area; last_flush starts a window.
// update uses steady_clock::now(); value(now) uses the same clock unless
// overridden. Piecewise-constant x between updates.
class TimeWeightedMean {
public:
  using Clock = std::chrono::steady_clock;
  static constexpr std::chrono::milliseconds kMetricsSnapshotPeriod{250};

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
#ifdef SIDECAR_METRICS
    if (metric_id != 0 && last_value_call_time_.has_value() &&
        (t - *last_value_call_time_) >= kMetricsSnapshotPeriod &&
        (!last_snapshot_time_.has_value() ||
         (t - *last_snapshot_time_) >= kMetricsSnapshotPeriod)) {
      metrics::record(metric_id, static_cast<float>(snapshot_mean(t)));
      last_snapshot_time_ = t;
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
#ifdef SIDECAR_METRICS
    if (metric_id == 0) {
      LOG(FATAL) << "Description of TwAvg is empty";
    }
    metrics::record(metric_id, static_cast<float>(area_));
    last_value_call_time_ = now;
    last_snapshot_time_ = now;
#endif
    return std::exchange(area_, 0.0);
  }

  void set_description(std::string desc) {
    description = desc;
#ifdef SIDECAR_METRICS
    metric_id = metrics::id("TwAvg " + description + " N:N");
#endif
  }

  bool empty() const { return !last_flush_.has_value(); }

private:
#ifdef SIDECAR_METRICS
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
#ifdef SIDECAR_METRICS
  uint16_t metric_id = 0;
  std::optional<Clock::time_point> last_value_call_time_;
  std::optional<Clock::time_point> last_snapshot_time_;
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

#ifdef SIDECAR_METRICS
  // Metric ids interned once at construction; only the method varies per RPC.
  struct RpcMetricIds {
    uint16_t e2e = 0;
    uint16_t req_for = 0;
    uint16_t res_for = 0;
    metrics::MethodCache methods;
  };
  LocalMap<RpcMetricIds> rpc_ids_ingress; // keyed by us (hosted) service
  LocalMap<RpcMetricIds> rpc_ids_egress;  // keyed by ds service
  LocalMap<uint16_t> limit_local_id;      // keyed by us service
  LocalMap<uint16_t> local_rt_id;         // keyed by us service
  uint16_t limit_global_id = 0;
#endif
};