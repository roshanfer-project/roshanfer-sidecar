#pragma once

#include "ingress.h"
#include "rpc_mapper.h"
#include "rpc_queue.h"
#include "udp_listener.h"
#include <buffer_manager.h>
#include <config.h>
#include <connection.h>
#include <listener.h>
#include <ring_wrapper.h>
#include <state.h>
#include <string>
#include <unordered_map>

class EventLoop {

public:
  EventLoop(int, std::string &, Config, SharedState &);
  void run();

public:
  int index;
  std::string &ingress_service;
  Config config;
  RingWrapper ring;
  BufferManager buffer_manager;
  std::unordered_map<ConnectionType, std::shared_ptr<Listener>> listeners;
  UDPListner udp_listener;
  RPCMapper rpc_mapper;
  RPCQueue rpc_queue;
  Ingress ingress;
  State state;
};