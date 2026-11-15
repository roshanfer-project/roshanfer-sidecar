#include "stats.h"
#include "config.h"
#include "connection_enums.h"
#include "fast_map.hpp"
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <ios>
#include <memory>

#ifdef SIDECAR_METRICS
#include "metrics.h"
#endif

MovingAverage::MovingAverage() : count(0), value(0.0) {}

void MovingAverage::update(int32_t new_value) {
  count++;
  value += ((float)new_value - value) / (float)count;
  VLOG(2) << "MovingAverage update: " << new_value << " count: " << count
          << " value: " << value;
  if (count % 1000 == 0) {
    VLOG(1) << "Stats: Moving average update. "
            << "| description: " << description << "| value: " << std::fixed
            << std::setprecision(4) << value;
#ifdef SIDECAR_METRICS
    if (metric_id != 0)
      metrics::record(metric_id, value);
#endif
  }
}

float MovingAverage::get_value() { return value; }

uint32_t MovingAverage::get_count() const { return count; }

ExponentialMovingAverage::ExponentialMovingAverage()
    : count(0), value(0.0), alpha(0.01F) {}

ExponentialMovingAverage::ExponentialMovingAverage(float alpha)
    : count(0), value(0.0), alpha(alpha) {}

void ExponentialMovingAverage::update(int32_t new_value) {
  count++;
  last_value = new_value;
  value = alpha * (float)new_value + (1 - alpha) * value;
  VLOG(2) << "ExponentialMovingAverage update: " << new_value
          << " count: " << count << " value: " << value;
  if (count % 1000 == 0) {
    VLOG(1) << "EMA: " << description << " value: " << value;
#ifdef SIDECAR_METRICS
    if (metric_id != 0)
      metrics::record(metric_id, value);
#endif
  }
}

void ExponentialMovingAverage::up() { update(last_value + 1); }

void ExponentialMovingAverage::down() {
  if (last_value == 0) {
    LOG(FATAL) << "Last value is 0, cannot decrement";
  }
  update(last_value - 1);
}

float ExponentialMovingAverage::get_value() { return value; }

float ExponentialMovingAverage::get_value_cap(float min, float max) {
  return std::max(min, std::min(max, value));
}

uint32_t ExponentialMovingAverage::get_count() const { return count; }

Counter::Counter(std::string desc) : count(0), description(desc) {}

void Counter::up(int32_t val) {
  count += val;
  if (count % 500 == 0) {
    VLOG(1) << "Stats: Counter update. "
            << "| description: " << description << "| count: " << count;
  }
}

void Counter::down(int32_t val) { count -= val; }

int32_t Counter::get_count() { return count; }

void TDigest::add(int32_t val) {
  td_add(td, val, 1);

#ifdef SIDECAR_METRICS
  count++;
  if (count % 2000 == 0) {
    if (p95_id == 0) {
      LOG(FATAL) << "description is not set for TD";
    }
    const int64_t now = metrics::now_us();
    metrics::record(p95_id, static_cast<float>(get_quantile(0.95)), 0, now);
    metrics::record(p99_id, static_cast<float>(get_quantile(0.99)), 0, now);
  }
#endif
}

Stats::Stats(std::vector<std::string> ds_services,
             std::vector<std::string> us_services)
    : mode2_credits("Mode2 Credits"), ema_ds_service_time_us(ds_services),
      ema_us_service_time_us(us_services), ema_us_sidecar_rtt_us(us_services),
      ema_ds_sidecar_rtt_us(ds_services), tail_ds_service_time_us(ds_services),
      tail_e2e_time_us(us_services), time_mean_ds_concurrency(ds_services),
      ema_wait_to_tx_us(ds_services)
#ifdef SIDECAR_METRICS
      ,
      rpc_ids_ingress(us_services), rpc_ids_egress(ds_services),
      limit_local_id(us_services), local_rt_id(us_services),
      limit_global_id(metrics::id("LIMIT GLOBAL T:T"))
#endif
{
#ifdef SIDECAR_METRICS
  for (const auto &service : us_services) {
    auto &ids = rpc_ids_ingress.get(service);
    ids.e2e = metrics::id("E2E INGRESS " + service);
    ids.req_for = metrics::id("REQ-FOR INGRESS " + service);
    ids.res_for = metrics::id("RES-FOR INGRESS " + service);
    limit_local_id.get(service) =
        metrics::id("LIMIT LOCAL-" + service + " T:T");
    local_rt_id.get(service) =
        metrics::id("Measured Local-RT-" + service + " T:T");
  }
  for (const auto &service : ds_services) {
    auto &ids = rpc_ids_egress.get(service);
    ids.e2e = metrics::id("E2E EGRESS " + service);
    ids.req_for = metrics::id("REQ-FOR EGRESS " + service);
    ids.res_for = metrics::id("RES-FOR EGRESS " + service);
  }
#endif
  for (const auto &service : us_services) {
    ema_us_sidecar_rtt_us.get(service).set_description("US-RTT-" + service);
    ema_us_service_time_us.get(service).set_description("US-RT-" + service);
    tail_e2e_time_us.get(service).set_description("E2E-" + service);
  }
  for (const auto &service : ds_services) {
    ema_ds_service_time_us.get(service).set_description("DS-RTT-" + service);
    ema_ds_service_time_us.get(service).set_description("DS-RT-" + service);
    tail_ds_service_time_us.get(service).set_description("DS-RT-" + service);
    time_mean_ds_concurrency.get(service).set_description("DSC-" + service);
    ema_wait_to_tx_us.get(service).set_description("DS-WAIT-TO-CREDIT-" +
                                                   service);
    ema_ds_sidecar_rtt_us.get(service).set_description("DS-RTT-" + service);
  }
}

void Stats::report_latency(const std::shared_ptr<RPCMessage> &rpc) {
  // For drops we only log metrics related to it and return as it won't update
  // other stats
  if (rpc->is_error()) {
#ifdef SIDECAR_METRICS
    // Drops are rare, and their service may be in neither list: intern here.
    std::string name = "DROP " + type_to_str(rpc->get_type()) + " " +
                       rpc->get_service() + ":";
    if (rpc->http() == HTTP::HTTP1) {
      auto http_rpc = std::dynamic_pointer_cast<HTTPMessage>(rpc);
      if (!http_rpc) {
        LOG(FATAL) << "Null pointer after dynamic_pointer_cast";
      }
      name += std::to_string(http_rpc->get_status());
    } else {
      name += rpc->get_method();
    }
    metrics::record(metrics::id(name), 1);
#endif
    return;
  }

  switch (rpc->get_type()) {
  case ConnectionType::EGRESS: {
    // EGRESS side should only update ds metrics

    if (rpc->req_for_time.time_since_epoch() >
        std::chrono::steady_clock::now().time_since_epoch()) {
      LOG(FATAL) << "Service time is negative";
    }
    auto ds_service_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - rpc->req_for_time)
            .count();
    ema_ds_service_time_us.get(rpc->get_service())
        .update(static_cast<int32_t>(ds_service_time));
    tail_ds_service_time_us.get(rpc->get_service())
        .update(static_cast<int32_t>(ds_service_time));

    if (config.is_ingress) {
      // calculate E2E for Ingress
      if (rpc->req_rcv_time.time_since_epoch() >
          std::chrono::steady_clock::now().time_since_epoch()) {
        LOG(FATAL) << "Service time is negative";
      }
      auto e2e_time = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - rpc->req_rcv_time)
                          .count();
      tail_e2e_time_us.get(rpc->get_service()).update((double)e2e_time);
    }

    VLOG(2) << "Stats: EGRESS Reported latency "
            << "| service: " << rpc->get_service()
            << "| service_time: " << ds_service_time;

    break;
  }
  case ConnectionType::INGRESS: {
    // INGRESS side should update both us and e2e metrics

    if (rpc->req_for_time.time_since_epoch() >
        std::chrono::steady_clock::now().time_since_epoch()) {
      LOG(FATAL) << "Service time is negative";
    }
    auto us_service_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - rpc->req_for_time)
            .count();
    ema_us_service_time_us.get(rpc->get_service())
        .update(static_cast<int32_t>(us_service_time));

    VLOG(2) << "Stats: INGRESS Reported latency "
            << "| service: " << rpc->get_service()
            << "| service_time: " << us_service_time;

    break;
  }
  default:
    LOG(FATAL) << "Unhandeld ConnectionType in Stats";
    break;
  }

#ifdef SIDECAR_METRICS
  {
    const auto now = std::chrono::steady_clock::now();
    if (rpc->req_rcv_time > now) {
      LOG(FATAL) << "Service time is negative";
    }
    auto us = [](auto d) {
      return static_cast<float>(
          std::chrono::duration_cast<std::chrono::microseconds>(d).count());
    };
    auto &ids = rpc->get_type() == ConnectionType::INGRESS
                    ? rpc_ids_ingress.get(rpc->get_service())
                    : rpc_ids_egress.get(rpc->get_service());
    const uint16_t mid = ids.methods.get(rpc->get_method());
    const int64_t ts = metrics::now_us();
    metrics::record(ids.e2e, us(now - rpc->req_rcv_time), mid, ts);
    metrics::record(ids.req_for, us(rpc->req_for_time - rpc->req_rcv_time),
                    mid, ts);
    metrics::record(ids.res_for, us(now - rpc->res_rcv_time), mid, ts);
  }
#endif
}
