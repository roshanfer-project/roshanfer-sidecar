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
                 Stats &stats_ref)
    : stats(stats_ref), queue(std::deque<std::shared_ptr<RPCMessage>>()),
      has_dn_on_fly(false), drop_id(0), drop_fd(-index_arg),
      last_rpc_id((RPCID)index_arg << 48),
      ingress_service(ingress_service_ref) {
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
  // set the dealine
  auto slo = config.routing.at(ingress_service).slo.value_or(0) * 1000;
  auto slack =
      slo - (int)std::ceil(
                stats.tail_ds_service_time_us.get(ingress_service).value());
  // apply guard
  slack -= (int)(slo * 0.05);
  if (slack < 0) {
    slack = slo;
  }

  rpc->deadline =
      std::chrono::steady_clock::now() + std::chrono::microseconds(slack);

  add_rpc_id_header(rpc);
  add_priority_header(rpc);
  queue.push_back(std::move(rpc));
  VLOG(2) << "Enqueued RPC message for service: " << ingress_service;

#ifdef NANO_LOG_ENABLED
  NANO_LOG(NOTICE, "M# %s Measured QS-%s T:T %zu", config.name.c_str(),
           ingress_service.c_str(), queue.size());
  NANO_LOG(NOTICE, "M# %s Measured Deadline-slack-%s T:T %d",
           config.name.c_str(), ingress_service.c_str(), slack);
#endif
}

std::optional<std::shared_ptr<RPCMessage>>
Ingress::dequeue(RPCQueue &rpc_queue, RPCMapper &rpc_mapper) {
  if (queue.empty()) {
    LOG(FATAL) << "No RPC message in ingress queue for service: "
               << ingress_service;
  }

  // run the tail-drop logic
  bool succuss = false;
  std::shared_ptr<RPCMessage> rpc;
  while (queue.size() != 0) {
    rpc = std::move(queue.front());
    queue.pop_front();

    // check the deadline
    if (std::chrono::steady_clock::now() > rpc->deadline) {
      // deadline missed
      drop_rpc(std::move(rpc), rpc_queue, rpc_mapper);
    } else {
      succuss = true;
      break;
    }
  }

  has_dn_on_fly = false;
  if (succuss) {
    VLOG(2) << "Dequeued RPC message for service: " << ingress_service;
#ifdef NANO_LOG_ENABLED
    NANO_LOG(NOTICE, "M# %s Measured QS-%s T:T %zu", config.name.c_str(),
             ingress_service.c_str(), queue.size());
#endif
    return rpc;
  } else {
    return std::nullopt;
  }
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

void Ingress::drop_rpc(std::shared_ptr<RPCMessage> rpc, RPCQueue &rpc_queue,
                       RPCMapper &rpc_mapper) {
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
          << "| service: " << ingress_service << "| id: " << drop_rpc->get_id();
}

bool Ingress::send_dn_checker() {
  bool check = (queue.size() > 0 && !has_dn_on_fly);
  has_dn_on_fly = check ? true : has_dn_on_fly;
  return check;
}

RPCID Ingress::get_tail_id() { return queue.front()->get_id(); }

Priority Ingress::get_tail_priority() { return queue.front()->get_priority(); }

size_t Ingress::size() { return queue.size(); }

void Ingress::dump_state() {
  LOG(INFO) << "--- Ingress Queue Sizes (ingress) ---";
  LOG(INFO) << "  " << ingress_service << ": " << queue.size();
  LOG(INFO) << "--- Drop ID (drop_id) ---";
  LOG(INFO) << "  " << ingress_service << ": " << drop_id;
}
