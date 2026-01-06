#include "ingress.h"
#include "config.h"
#include "connection_enums.h"
#include "fast_map.hpp"
#include "glog/logging.h"
#include "rpc_mapper.h"
#include "rpc_message.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>

#if defined(NANO_LOG_ENABLED) || defined(NABO_LOG_TRACE_ENABLED)
#include "NanoLog.h"
#include "NanoLogCpp17.h"

using namespace NanoLog::LogLevels;
#endif

Ingress::Ingress(int index_arg, std::string &ingress_service_ref)
    : queue(std::deque<std::shared_ptr<RPCMessage>>()), drop_id(0),
      drop_fd(-index_arg), max_th_us(), min_th_us(), red_count(-1), gen(rd()),
      dis(0.0, 1.0), last_rpc_id(0), ingress_service(ingress_service_ref) {
  if (config.is_ingress) {
    if (!config.routing.at(ingress_service_ref).slo.has_value()) {
      LOG(FATAL) << "No SLO configured for ingress service: "
                 << ingress_service_ref;
    }
    max_th_us =
        (float)config.routing.at(ingress_service_ref).slo.value() * 1000 * 0.8F;
    min_th_us =
        (float)config.routing.at(ingress_service_ref).slo.value() * 1000 * 0.3F;
    LOG(INFO) << "Ingress: " << ingress_service << " max_th_us: " << max_th_us
              << " min_th_us: " << min_th_us;
  }
}

Ingress::~Ingress() {}

void Ingress::enqueue(std::shared_ptr<RPCMessage> rpc) {
  if (rpc->get_service() != ingress_service) {
    LOG(FATAL) << "Service mismatch in ingress: expected " << ingress_service
               << ", got " << rpc->get_service();
  }
  VLOG(2) << "Enqueued RPC message for service: " << rpc->get_service();
  queue.push_back(std::move(rpc));
}

std::shared_ptr<RPCMessage> Ingress::dequeue() {
  if (queue.empty()) {
    LOG(FATAL) << "No RPC message in ingress queue for service: "
               << ingress_service;
  }
  auto rpc = std::move(queue.front());
  queue.pop_front();

  VLOG(2) << "Dequeued RPC message for service: " << ingress_service;
  return rpc;
}

void Ingress::add_rpc_id_header(std::shared_ptr<RPCMessage> &rpc) {
  // convert last_rpc_id to a char array
  // reset the array
  rpc_id_header_value.fill(0);
  // update last_rpc_id and convert to a char array
  last_rpc_id++;
  // convert last_rpc_id to a char array
  std::snprintf(rpc_id_header_value.data(), rpc_id_header_value.size(), "%d",
                last_rpc_id);
  rpc->add_header_field(
      RPC_ID_HEADER_NAME, RPC_ID_HEADER_NAME_LEN,
      reinterpret_cast<const uint8_t *>(rpc_id_header_value.data()),
      rpc_id_header_value.size(), true, false);
}

int64_t Ingress::add_to_be_admitted_or_drop(RPCQueue &rpc_queue,
                                            RPCMapper &rpc_mapper,
                                            int32_t e2e_delay) {
  bool drop = false;
  if ((float)e2e_delay >= min_th_us && (float)e2e_delay <= max_th_us) {
    red_count++;
    float max_p = 1.0F;
    float pb = max_p * ((float)e2e_delay - min_th_us) / (max_th_us - min_th_us);
    float uni_rv = (float)dis(gen);
    drop = uni_rv < pb;
    if (drop) {
      red_count = 0;
    }
#ifdef NANO_LOG_ENABLED
    NANO_LOG(NOTICE, "M# %s Prob Pb-%s T:T %f", config.name.c_str(),
             ingress_service.c_str(), pb);
#endif
  } else if ((float)e2e_delay >= max_th_us) {
    red_count = 0;
    drop = true;
  } else {
    red_count = -1;
    drop = false;
  }

  if (drop) {
    // dropping the request
    auto drop_rpc =
        std::dynamic_pointer_cast<HTTPMessage>(std::move(queue.back()));
    if (!drop_rpc) {
      LOG(FATAL) << "Null pointer after dynamic_pointer_cast"
                 << " service: " << ingress_service
                 << " queue size: " << queue.size();
    }
    queue.pop_back();

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
            << "| service: " << ingress_service
            << "| id: " << drop_rpc->get_id() << "| e2e delay: " << e2e_delay;
  } else {
    // accepting the request
    auto rpc = dequeue();
    add_rpc_id_header(rpc);
    rpc_queue.enqueue(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM,
                      rpc->get_ds_fd(), rpc->get_ds_stream_id());
    VLOG(2) << "INGRESS: New request to be admitted "
            << "| service: " << ingress_service << "| id: " << rpc->get_id()
            << "| e2e delay: " << e2e_delay;
  }

  return drop == false;
}

size_t Ingress::size() { return queue.size(); }

void Ingress::dump_state() {
  LOG(INFO) << "--- Ingress Queue Sizes (ingress) ---";
  LOG(INFO) << "  " << ingress_service << ": " << queue.size();
  LOG(INFO) << "--- Drop ID (drop_id) ---";
  LOG(INFO) << "  " << ingress_service << ": " << drop_id;
}
