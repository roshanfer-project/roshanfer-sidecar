#include "stats.h"
#include "fast_map.hpp"
#include "hdr/hdr_histogram.h"
#include <chrono>
#include <cstdint>
#include <ios>

Utilization::Utilization(uint32_t period_count, std::vector<std::string> services)
:   total(LocalMap<double>(services)),
    last_in(LocalMap<uint32_t>(services)),
    count(LocalMap<uint32_t>(services)),
    period(period_count),
    last_update(LocalMap<std::chrono::steady_clock::time_point>(services)),
    last_report(LocalMap<std::chrono::steady_clock::time_point>(services)) {
        for (const auto& service : services) {
            last_update.set(service, std::chrono::steady_clock::now());
            last_report.set(service, std::chrono::steady_clock::now());
        }
        hdr_init(1, 100, 3, &hist);
    }

void Utilization::update(uint32_t in, std::string& service) {
    // calculate the time difference since the last report in milliseconds
    auto now = std::chrono::steady_clock::now();
    double time_diff_ms = (double)std::chrono::duration_cast<std::chrono::microseconds>(now - last_update.get(service)).count();

    total.add(service, (double)last_in.get(service) * time_diff_ms);
    last_in.set(service, in);
    last_update.set(service, now);
    count.add(service, 1);
    hdr_record_value(hist, (int64_t)in);

    VLOG(2) << "Updated utilization for service: " << service
           << ", in: " << (double)in
           << ", total: " << std::fixed << std::setprecision(4) << total.get(service)
           << ", count: " << count.get(service)
           << ", time_diff_ms: " << time_diff_ms;
    if (count.get(service) >= period) {
        report(service);
    }
}

void Utilization::report(std::string& service) {
    double time_diff_ms = (double)std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - last_report.get(service)).count();
    double utilization = total.get(service) / time_diff_ms;

    VLOG(2) << "Reported utilization for service: " << service
           << ", utilization: " << std::fixed << std::setprecision(4) << utilization
           << ", time_diff_ms: " << time_diff_ms
           << ", total: " << std::fixed << std::setprecision(4) << total.get(service)
           << ", count: " << count.get(service);

    // template: U# <sidecar name> UTILIZATION <service> <utilization>
    /* NANO_LOG(NOTICE, "U# %s UTILIZATION %s %.5f",
        config.name.c_str(), service.c_str(), utilization); */
    total.set(service, 0.0);
    count.set(service, 0);
    last_in.set(service, 0);
    last_report.set(service, std::chrono::steady_clock::now());

    auto p50 =  hdr_value_at_percentile(hist, 50);
    auto p70 =  hdr_value_at_percentile(hist, 70.0);
    auto p90 =  hdr_value_at_percentile(hist, 90.0);
    auto p100 =  hdr_value_at_percentile(hist, 100.0);
    hdr_reset(hist);
    //NANO_LOG(NOTICE, "UTILIZATION P50 %ld P70 %ld P90 %ld P100 %ld", p50, p70, p90, p100);
    VLOG(1) << "Stats: Reported utilization for service: " << service
            << ", utilization: " << std::fixed << std::setprecision(4) << utilization
            << ", p50: " << p50
            << ", p70: " << p70
            << ", p90: " << p90
            << ", p100: " << p100;
}


MovingAverage::MovingAverage() : count(0), value(0.0) {}

void MovingAverage::update(int32_t new_value) {
    count++;
    value += ((float)new_value - value) / (float)count;
    VLOG(2) << "MovingAverage update: " << new_value << " count: " << count << " value: " << value;
    if (count % 1000 == 0) {
        VLOG(1) << "Stats: Moving average update. "
        << "| description: " << description
        << "| value: " << std::fixed << std::setprecision(4) << value;
    }
}

float MovingAverage::get_value() {
    return value;
}

uint32_t MovingAverage::get_count() const {
    return count;
}

ExponentialMovingAverage::ExponentialMovingAverage() : count(0), value(0.0), alpha(0.15F) {}

ExponentialMovingAverage::ExponentialMovingAverage(float alpha) : count(0), value(0.0), alpha(alpha) {}

void ExponentialMovingAverage::update(int32_t new_value) {
    count++;
    last_value = new_value;
    value = alpha * (float)new_value + (1 - alpha) * value;
    VLOG(2) << "ExponentialMovingAverage update: " << new_value << " count: " << count << " value: " << value;
    if (count % 1000 == 0) {
        VLOG(1) << "Stats: Exponential moving average update. "
        << "| description: " << description
        << "| alpha: " << std::fixed << std::setprecision(2) << alpha
        << "| value: " << std::fixed << std::setprecision(4) << value
        << "| count: " << count;
    }
}

void ExponentialMovingAverage::up() {
    update(last_value+1);
}

void ExponentialMovingAverage::down() {
    if (last_value == 0) {
        LOG(FATAL) << "Last value is 0, cannot decrement";
    }
    update(last_value-1);
}

float ExponentialMovingAverage::get_value() {
    return value;
}

uint32_t ExponentialMovingAverage::get_count() const {
    return count;
}

Stats::Stats(std::vector<std::string> services)
: hist(nullptr), avg_service_time_us(services) {}

void Stats::update_hist(struct hdr_histogram* new_hist) {
    this->hist = new_hist;
}

void Stats::report_latency(const std::shared_ptr<RPCMessage>& rpc) {
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - rpc->req_rcv_time);
    
    // update hist only if we are Ingress and also just for E2E Ingress requests
    if (config.is_ingress) {
        bool ret = hdr_record_value(hist, static_cast<int64_t>(duration.count()));
        if (!ret) {
            LOG(FATAL) << "Failed to record value: " << static_cast<int64_t>(duration.count());
        }
        avg_service_time_us.get(rpc->get_service()).update(static_cast<int32_t>(duration.count()));

        VLOG(2) << "Stats: Reported latency "
                << "| service: " << rpc->get_service()
                << "| duration: " << duration.count();
    }
}