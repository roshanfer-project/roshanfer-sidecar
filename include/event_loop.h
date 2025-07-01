#pragma once

#include "ingress.h"
#include "rpc_mapper.h"
#include "rpc_queue.h"
#include "udp_listener.h"
#include <connection.h>
#include <buffer_manager.h>
#include <ring_wrapper.h>
#include <config.h>
#include <listener.h>
#include <state.h>
#include <unordered_map>

class EventLoop {

public:
    EventLoop(Config);
    void run();

public:
    Config config;
    RingWrapper ring;
    BufferManager buffer_manager;
    std::unordered_map<ConnectionType, Listener> listeners;
    UDPListner udp_listener;
    RPCMapper rpc_mapper;
    RPCQueue rpc_queue;
    Ingress ingress;
    State state;
};