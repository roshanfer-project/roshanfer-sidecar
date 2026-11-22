#include "rpc_message.h"

#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

const uint8_t *u8(const char *s) { return reinterpret_cast<const uint8_t *>(s); }

void add_req_header(RPCMessage &rpc, const char *name, const char *value) {
  rpc.add_header_field(u8(name), std::strlen(name), u8(value),
                       std::strlen(value), true, false);
}

// RPCMessage's destructor FATALs, so the pool and its messages are never freed.
RPCMessagePool &pool() {
  static auto *p = new RPCMessagePool(8, 8);
  return *p;
}

// Holds every message a test takes, so none reaches the FATAL destructor.
void keep(std::shared_ptr<RPCMessage> rpc) {
  static auto *kept = new std::vector<std::shared_ptr<RPCMessage>>();
  kept->push_back(std::move(rpc));
}

template <typename Msg>
std::shared_ptr<Msg> get(HTTP http, ConnectionType type) {
  auto rpc = pool().get_rpc(1, 1, http, type);
  keep(rpc);
  return std::dynamic_pointer_cast<Msg>(rpc);
}

TEST(Priority, UnsetOnFreshAndClearedMessage) {
  for (HTTP http : {HTTP::HTTP1, HTTP::HTTP2}) {
    auto rpc = pool().get_rpc(1, 1, http, ConnectionType::INGRESS);
    EXPECT_EQ(rpc->get_priority(), -1);
    add_req_header(*rpc, "priority", "1");
    EXPECT_EQ(rpc->get_priority(), 1);
    pool().free_rpc(std::move(rpc));
    rpc = pool().get_rpc(1, 1, http, ConnectionType::INGRESS);
    // A pool is FIFO: cycle until the freed message comes back.
    for (int i = 0; i < 8; i++) {
      EXPECT_EQ(rpc->get_priority(), -1);
      pool().free_rpc(std::move(rpc));
      rpc = pool().get_rpc(1, 1, http, ConnectionType::INGRESS);
    }
    keep(std::move(rpc));
  }
}

// Fields shared by both message types: dirty every one, then expect clear() to
// restore the constructor's values.
void dirty_common(RPCMessage &rpc) {
  rpc.set_ds_stream_id(7);
  rpc.set_ds_fd(7);
  rpc.set_us_stream_id(7);
  rpc.set_us_fd(7);
  rpc.set_error(true);
  add_req_header(rpc, "rpc-id", "42"); // sets global_id and local_id
  add_req_header(rpc, "priority", "1");
  add_req_header(rpc, "deadline", "99");
  rpc.add_header_field(u8("x-res"), 5, u8("v"), 1, false, false);
  rpc.req_rcv_time = std::chrono::steady_clock::now();
  rpc.req_for_time = rpc.req_rcv_time;
  rpc.res_rcv_time = rpc.req_rcv_time;
  rpc.pfanout_req = 3;
  rpc.pfanout_res = 2;
  static std::string svc = "svc";
  rpc.dfanout_service = &svc;
  ASSERT_NE(rpc.get_local_id(), -1);
}

void expect_common_cleared(RPCMessage &rpc) {
  EXPECT_EQ(rpc.get_ds_stream_id(), -1);
  EXPECT_EQ(rpc.get_ds_fd(), -1);
  EXPECT_EQ(rpc.get_us_stream_id(), -1);
  EXPECT_EQ(rpc.get_us_fd(), -1);
  EXPECT_FALSE(rpc.is_error());
  EXPECT_EQ(rpc.get_local_id(), -1);
  EXPECT_EQ(rpc.get_id_string(), "(global: -1, local: -1)");
  EXPECT_EQ(rpc.get_priority(), -1);
  EXPECT_EQ(rpc.deadline, 0);
  EXPECT_TRUE(rpc.get_service().empty());
  EXPECT_TRUE(rpc.get_method().empty());
  using TP = std::chrono::time_point<std::chrono::steady_clock>;
  EXPECT_EQ(rpc.req_rcv_time, TP());
  EXPECT_EQ(rpc.req_for_time, TP());
  EXPECT_EQ(rpc.res_rcv_time, TP());
  EXPECT_EQ(rpc.pfanout_req, 0);
  EXPECT_EQ(rpc.pfanout_res, 0);
  EXPECT_EQ(rpc.dfanout_service, nullptr);
  EXPECT_TRUE(rpc.credit_return_queue.empty());
}

TEST(Clear, GrpcResetsEverything) {
  auto rpc = get<gRPCMessage>(HTTP::HTTP2, ConnectionType::INGRESS);
  dirty_common(*rpc);
  rpc->add_header_field(u8(":path"), 5, u8("/svc/m"), 6, true, false);
  rpc->set_method("m");
  rpc->add_header_field(u8("grpc-status"), 11, u8("0"), 1, false, true);
  rpc->add_data(u8("req"), 3, true);
  rpc->add_data(u8("res"), 3, false);

  rpc->clear();

  expect_common_cleared(*rpc);
  EXPECT_EQ(rpc->get_req_header_count(), 0u);
  EXPECT_EQ(rpc->get_res_header_count(), 0u);
  EXPECT_EQ(rpc->get_res_trailer_count(), 0u);
  for (uint8_t key : {uint8_t{0}, uint8_t{1}}) {
    EXPECT_EQ(rpc->get_data_map().at(key)->offset, 0u);
    EXPECT_EQ(rpc->get_data_map().at(key)->read_offset, 0u);
  }
}

TEST(Clear, HttpResetsEverything) {
  auto rpc = get<HTTPMessage>(HTTP::HTTP1, ConnectionType::INGRESS);
  dirty_common(*rpc);
  rpc->set_method("GET", 3);
  rpc->set_service("/svc/m", 6);
  rpc->set_path("/svc/m", 6);
  rpc->set_minor(1);
  rpc->set_status(503);
  rpc->set_msg("busy", 4);
  rpc->add_data(u8("res"), 3, false);
  rpc->get_res_data().read_offset = 1;

  rpc->clear();

  expect_common_cleared(*rpc);
  EXPECT_EQ(rpc->get_req_header_count(), 0u);
  EXPECT_EQ(rpc->get_res_header_count(), 0u);
  EXPECT_TRUE(rpc->get_path().empty());
  EXPECT_EQ(rpc->get_minor(), 0);
  EXPECT_EQ(rpc->get_status(), 0);
  EXPECT_TRUE(rpc->get_msg().empty());
  EXPECT_FALSE(rpc->is_drop());
  EXPECT_EQ(rpc->get_res_data().offset, 0u);
  EXPECT_EQ(rpc->get_res_data().read_offset, 0u);
}

} // namespace
