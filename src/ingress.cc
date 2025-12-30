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

Ingress::Ingress(int index_arg, std::string &ingress_service_ref)
    : p95_us(-1), p50_us(-1), queue(std::deque<std::shared_ptr<RPCMessage>>()),
      drop_id(0), drop_fd(-index_arg), slo_us(), last_rpc_id(0),
      ingress_service(ingress_service_ref) {
  if (config.is_ingress) {
    if (!config.routing.at(ingress_service_ref).slo.has_value()) {
      LOG(FATAL) << "No SLO configured for ingress service: "
                 << ingress_service_ref;
    }
    slo_us = config.routing.at(ingress_service_ref).slo.value() * 1000;
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
                                            int32_t queueing_delay) {

  int new_requests = (int)queue.size();
  int new_added = 0;

  while (new_requests > 0 && queueing_delay < slo_us) {
    auto rpc = dequeue();
    add_rpc_id_header(rpc);
    rpc_queue.enqueue(ConnectionType::EGRESS, ConnectionDirection::DOWNSTREAM,
                      rpc->get_ds_fd(), rpc->get_ds_stream_id());
    new_added++;
    new_requests--;
    VLOG(2) << "INGRESS: New request to be admitted "
            << "| service: " << ingress_service << "| id: " << rpc->get_id()
            << "| queueing delay: " << queueing_delay;
  }

  // drop remaining requests
  while (new_requests > 0) {
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
            << "| id: " << drop_rpc->get_id()
            << "| queueing delay: " << queueing_delay;
    new_requests--;
  }

  return new_added;
}

void Ingress::update_stats(int32_t new_p50_us, int32_t new_p95_us) {
  p50_us = new_p50_us;
  p95_us = new_p95_us;
  VLOG(1) << "Ingress: Updated p50_us to " << new_p50_us << " and p95_us to "
          << new_p95_us << " for service: " << ingress_service;
}

size_t Ingress::size() { return queue.size(); }

void Ingress::dump_state() {
  LOG(INFO) << "--- Ingress Queue Sizes (ingress) ---";
  LOG(INFO) << "  " << ingress_service << ": " << queue.size();
  LOG(INFO) << "--- Drop ID (drop_id) ---";
  LOG(INFO) << "  " << ingress_service << ": " << drop_id;
  LOG(INFO) << "--- P95 US (p95_us) ---";
  LOG(INFO) << "  " << ingress_service << ": " << p95_us;
  LOG(INFO) << "--- P50 US (p50_us) ---";
  LOG(INFO) << "  " << ingress_service << ": " << p50_us;
  LOG(INFO) << "--- SLO US (slo_us) ---";
  LOG(INFO) << "  " << ingress_service << ": " << slo_us;
}
