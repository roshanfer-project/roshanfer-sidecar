#include "ingress.h"
#include "config.h"
#include "connection_enums.h"
#include "fast_map.hpp"
#include "glog/logging.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include "rpc_queue.h"
#include "stats.h"
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#if defined(NANO_LOG_ENABLED) || defined(NABO_LOG_TRACE_ENABLED)
#include "NanoLog.h"
#include "NanoLogCpp17.h"

using namespace NanoLog::LogLevels;
#endif

Ingress::Ingress(int index_arg, std::string &ingress_service_ref,
                 Stats &stats_ref, RPCMapper &rpc_mapper_ref,
                 RPCQueue &rpc_queue_ref)
    : stats(stats_ref), rpc_mapper(rpc_mapper_ref), rpc_queue(rpc_queue_ref),
      queue(std::deque<std::shared_ptr<RPCMessage>>()), has_dn_on_fly(false),
      drop_id(0), drop_fd(-index_arg), last_rpc_id((RPCID)index_arg << 48),
      ingress_service(ingress_service_ref) {
  ingress_mean.set_description("Ingress-Mean-" + ingress_service);
  if (config.is_ingress) {
    if (!config.routing.at(ingress_service_ref).slo.has_value()) {
      LOG(FATAL) << "No SLO configured for ingress service: "
                 << ingress_service_ref;
    }
    if (!config.routing.at(ingress_service_ref).priority.has_value()) {
      LOG(FATAL) << "No priority configured for ingress service: "
                 << ingress_service_ref;
    } else {
      priority = config.routing.at(ingress_service_ref).priority.value();
      if (priority < 0 || priority > 3) {
        LOG(FATAL) << "Invalid priority configured for ingress service: "
                   << ingress_service_ref;
      }
    }
  }
}

Ingress::~Ingress() {}

void Ingress::enqueue(std::shared_ptr<RPCMessage> rpc) {
  if (rpc->get_service() != ingress_service) {
    LOG(FATAL) << "Service mismatch in ingress: expected " << ingress_service
               << ", got " << rpc->get_service();
  }

  // run the head-drop hook, if applicable
  if (queue.size() >= ingress_size_cap) {
    drop_rpc(std::move(rpc));
    return;
  }

  add_rpc_id_header(rpc);
  add_priority_header(rpc);
  queue.push_back(std::move(rpc));
  ingress_mean.update((double)queue.size());
  VLOG(2) << "Enqueued RPC message for service: " << ingress_service;

#ifdef NANO_LOG_ENABLED
  NANO_LOG(NOTICE, "M# %s Measured QS-%s T:T %zu", config.name.c_str(),
           ingress_service.c_str(), queue.size());
#endif
}

std::optional<std::shared_ptr<RPCMessage>> Ingress::dequeue() {
  if (queue.empty()) {
    LOG(FATAL) << "No RPC message in ingress queue for service: "
               << ingress_service;
  }

  auto rpc = std::move(queue.front());
  queue.pop_front();

  has_dn_on_fly = false;
  ingress_mean.update((double)queue.size());
  VLOG(2) << "Dequeued RPC message for service: " << ingress_service;
#ifdef NANO_LOG_ENABLED
  NANO_LOG(NOTICE, "M# %s Measured QS-%s T:T %zu", config.name.c_str(),
           ingress_service.c_str(), queue.size());
#endif
  return rpc;
}

void Ingress::update_ingress_cap() {
  auto slo_us = config.routing.at(ingress_service).slo.value_or(0) * 1000;
  auto err =
      (stats.tail_e2e_time_us.get(ingress_service).value() - slo_us) / slo_us;

  if (err > aimd_err_d) {
    ingress_size_cap = (size_t)std::ceil((float)ingress_size_cap / aimd_adj_d);
  } else if (err < aimd_err_i) {
    auto ing_mean = ingress_mean.value();
    auto concurrency =
        stats.time_mean_ds_concurrency.get(ingress_service).value();
    if (ing_mean >= (float)ingress_size_cap * aimd_queue_th) {
      ingress_size_cap += (size_t)std::ceil((-err) * aimd_adj_i);
    } else if ((float)ingress_size_cap > safe_multiply * concurrency) {
      const double lowered =
          std::ceil((double)ingress_size_cap / (double)aimd_adj_d);
      ingress_size_cap =
          (size_t)std::max((double)(safe_multiply * concurrency), lowered);
    }
  }

  ingress_size_cap = std::max((size_t)1, ingress_size_cap);
#ifdef NANO_LOG_ENABLED
  NANO_LOG(NOTICE, "M# %s Measured QS-CAP-%s T:T %zu", config.name.c_str(),
           ingress_service.c_str(), ingress_size_cap);
  NANO_LOG(NOTICE, "M# %s Measured ERR-%s N:N %f", config.name.c_str(),
           ingress_service.c_str(), err);
#endif
}

void Ingress::add_rpc_id_header(std::shared_ptr<RPCMessage> &rpc) {
  // convert last_rpc_id to a char array
  // reset the array
  rpc_id_header_value.fill(0);
  // update last_rpc_id and convert to a char array
  last_rpc_id++;
  // convert last_rpc_id to a char array
  std::snprintf(rpc_id_header_value.data(), rpc_id_header_value.size(), "%lld",
                (long long)last_rpc_id);
  rpc->add_header_field(
      RPC_ID_HEADER_NAME, RPC_ID_HEADER_NAME_LEN,
      reinterpret_cast<const uint8_t *>(rpc_id_header_value.data()),
      rpc_id_header_value.size(), true, false);
}

void Ingress::add_priority_header(std::shared_ptr<RPCMessage> &rpc) {
  priority_header_value.fill(0);
  std::snprintf(priority_header_value.data(), priority_header_value.size(),
                "%d", priority);
  rpc->add_header_field(
      PRIORITY_HEADER_NAME, PRIORITY_HEADER_NAME_LEN,
      reinterpret_cast<const uint8_t *>(priority_header_value.data()),
      priority_header_value.size(), true, false);
}

void Ingress::drop_rpc(std::shared_ptr<RPCMessage> rpc) {
  auto drop_rpc = std::dynamic_pointer_cast<HTTPMessage>(std::move(rpc));
  if (!drop_rpc) {
    LOG(FATAL) << "Null pointer after dynamic_pointer_cast"
               << " service: " << ingress_service;
  }
  if (drop_rpc->get_service() != ingress_service) {
    LOG(FATAL) << "Service mismatch in drop RPC: expected " << ingress_service
               << ", got " << drop_rpc->get_service();
  }

  drop_rpc->set_error(true);
  drop_rpc->set_status(503);
  drop_id++;
  drop_rpc->set_local_id(-(drop_id + 1));
  drop_rpc->set_us_fd(drop_fd);
  drop_rpc->set_us_stream_id(drop_id);
  rpc_mapper.route(ConnectionType::EGRESS, drop_rpc->get_ds_stream_id(),
                   drop_rpc->get_ds_fd(), drop_rpc->get_us_stream_id(),
                   drop_fd);
  rpc_queue.enqueue(
      ConnectionType::EGRESS,
      ConnectionDirection::UPSTREAM, // we want this to be forwarded to
                                     // downstream connection
      drop_rpc->get_us_fd(), drop_rpc->get_us_stream_id());

  VLOG(2) << "INGRESS: Dropped request "
          << "| service: " << ingress_service
          << "| id: " << drop_rpc->get_id_string();
}

bool Ingress::send_dn_checker() {
  bool check = (queue.size() > 0 && !has_dn_on_fly);
  has_dn_on_fly = check ? true : has_dn_on_fly;
  return check;
}

RPCID Ingress::get_tail_id() { return queue.front()->get_local_id(); }

Priority Ingress::get_tail_priority() { return queue.front()->get_priority(); }

size_t Ingress::size() { return queue.size(); }

void Ingress::dump_state() {
  LOG(INFO) << "--- Ingress Queue Sizes (ingress) ---";
  LOG(INFO) << "  " << ingress_service << ": " << queue.size();
  LOG(INFO) << "--- Drop ID (drop_id) ---";
  LOG(INFO) << "  " << ingress_service << ": " << drop_id;
}
