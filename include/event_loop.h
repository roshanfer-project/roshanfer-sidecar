#pragma once

#include "grpc_parser.h"
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
    void add_accept_submissions();

private:
    RingWrapper ring;
    BufferManager buffer_manager;
    std::unordered_map<ConnectionType, Listener> listeners;
    State state;
    UDPListner udp_listener;
    gRPCParser grpc_parser;
};