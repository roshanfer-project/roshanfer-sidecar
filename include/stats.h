#pragma once

#include "fast_map.hpp"
#include "hdr/hdr_histogram.h"
#include "rpc_message.h"
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

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

class Stats {
public:
  Stats(std::vector<std::string> ds_services,
        std::vector<std::string> us_services);

  // delete copy semantics
  Stats(const Stats &) = delete;
  Stats &operator=(const Stats &) = delete;

  // delete move semantics
  Stats(Stats &&) = delete;
  Stats &operator=(Stats &&) = delete;

  void report_latency(const std::shared_ptr<RPCMessage> &rpc);
  void update_hist(struct hdr_histogram *);
  LocalMap<ExponentialMovingAverage> &get_ds_avg_service_time_us() {
    return ds_avg_service_time_us;
  }
  LocalMap<ExponentialMovingAverage> &get_us_avg_service_time_us() {
    return us_avg_service_time_us;
  }

public:
  Counter mode2_credits;

private:
  struct hdr_histogram *hist;
  LocalMap<ExponentialMovingAverage> ds_avg_service_time_us;
  LocalMap<ExponentialMovingAverage> us_avg_service_time_us;
};