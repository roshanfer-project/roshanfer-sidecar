#include "rpc_queue.h"
#include "connection_enums.h"
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include "glog/logging.h"

RPCQueue::RPCQueue()
: queue_map(std::unordered_map<ConnectionType,
     std::unordered_map<ConnectionDirection, std::queue<std::tuple<int, int32_t>>>>()) {
    queue_map.emplace(ConnectionType::INGRESS, std::unordered_map<ConnectionDirection, std::queue<std::tuple<int, int32_t>>>());
    queue_map.emplace(ConnectionType::EGRESS, std::unordered_map<ConnectionDirection, std::queue<std::tuple<int, int32_t>>>());
    queue_map.at(ConnectionType::INGRESS).emplace(ConnectionDirection::UPSTREAM, std::queue<std::tuple<int, int32_t>>());
    queue_map.at(ConnectionType::INGRESS).emplace(ConnectionDirection::DOWNSTREAM, std::queue<std::tuple<int, int32_t>>());
    queue_map.at(ConnectionType::EGRESS).emplace(ConnectionDirection::UPSTREAM, std::queue<std::tuple<int, int32_t>>());
    queue_map.at(ConnectionType::EGRESS).emplace(ConnectionDirection::DOWNSTREAM, std::queue<std::tuple<int, int32_t>>());
}

void RPCQueue::enqueue(ConnectionType type, ConnectionDirection direction, int fd, int32_t stream_id) {
    queue_map.at(type).at(direction).push(std::make_tuple(fd, stream_id));
    VLOG(1) << "Enqueued (" << fd << "," << stream_id << ") on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
}

std::tuple<int, int32_t> RPCQueue::dequeue(ConnectionType type, ConnectionDirection direction) {
    if (queue_map.at(type).at(direction).empty()) {
        LOG(FATAL) << "Trying to dequeue from an empty queue";
    }
    auto tup = queue_map.at(type).at(direction).front();
    
    queue_map.at(type).at(direction).pop();
    VLOG(1) << "Dequeued (" << std::get<0>(tup) << "," << std::get<1>(tup) << ") on type " 
        << type_to_str(type) << " and direction " << direction_to_str(direction);
    return tup;
}

bool RPCQueue::empty(ConnectionType type, ConnectionDirection direction) {
    return queue_map.at(type).at(direction).empty();
}

size_t RPCQueue::size(ConnectionType type, ConnectionDirection direction) {
    return queue_map.at(type).at(direction).size();
}

