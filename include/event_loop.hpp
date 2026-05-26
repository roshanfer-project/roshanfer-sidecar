#pragma once

#include "ingress.hpp"
#include "rpc_mapper.hpp"
#include "rpc_queue.hpp"
#include <buffer_manager.hpp>
#include <config.hpp>
#include <connection.hpp>
#include <listener.hpp>
#include <ring_wrapper.hpp>
#include <state.hpp>
#include <string>
#include <unordered_map>

class EventLoop {

public:
  EventLoop(int, std::string &, const Config&, SharedState &);
  void run();

public:
  int index;
  std::string &ingress_service;
  Config config;
  RingWrapper ring;
  BufferManager buffer_manager;
  std::unordered_map<ConnectionType, std::shared_ptr<Listener>> listeners;
  RPCMapper rpc_mapper;
  RPCQueue rpc_queue;
  Stats stats;
  Ingress ingress;
  State state;
};