#pragma once

#include <connection.h>
#include <buffer_manager.h>
#include <ring_wrapper.h>
#include <config.h>
#include <listener.h>
#include <state.h>

class EventLoop {

public:
    EventLoop(Config);
    void run();
    void add_accept_submissions();

private:
    RingWrapper ring;
    BufferManager buffer_manager;
    //Listener ingress_listener;
    Listener egress_listener;
    State state;
};