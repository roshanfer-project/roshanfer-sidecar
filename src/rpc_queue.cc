#include "rpc_queue.h"
#include "connection_enums.h"
#include <unordered_map>
#include "glog/logging.h"

RPCQueue::RPCQueue()
: queue_map(std::unordered_map<ConnectionType,
     std::unordered_map<ConnectionDirection, std::queue<uint32_t>>>()) {
    queue_map.emplace(ConnectionType::INGRESS, std::unordered_map<ConnectionDirection, std::queue<uint32_t>>());
    queue_map.emplace(ConnectionType::EGRESS, std::unordered_map<ConnectionDirection, std::queue<uint32_t>>());
    queue_map[ConnectionType::INGRESS].emplace(ConnectionDirection::UPSTREAM, std::queue<uint32_t>());
    queue_map[ConnectionType::INGRESS].emplace(ConnectionDirection::DOWNSTREAM, std::queue<uint32_t>());
    queue_map[ConnectionType::EGRESS].emplace(ConnectionDirection::UPSTREAM, std::queue<uint32_t>());
    queue_map[ConnectionType::EGRESS].emplace(ConnectionDirection::DOWNSTREAM, std::queue<uint32_t>());
}

void RPCQueue::enqueue(ConnectionType type, ConnectionDirection direction, uint32_t stream_id) {
    queue_map[type][direction].push(stream_id);
    DLOG(INFO) << "Enqueued stream " << stream_id << " on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
}

uint32_t RPCQueue::dequeue(ConnectionType type, ConnectionDirection direction) {
    if (queue_map[type][direction].empty()) {
        LOG(FATAL) << "Trying to dequeue from an empty queue";
    }
    uint32_t stream_id = queue_map[type][direction].front();
    queue_map[type][direction].pop();
    DLOG(INFO) << "Dequeued stream " << stream_id << " on type " << type_to_str(type) << " and direction " << direction_to_str(direction);
    return stream_id;
}

bool RPCQueue::empty(ConnectionType type, ConnectionDirection direction) {
    return queue_map[type][direction].empty();
}

int RPCQueue::size(ConnectionType type, ConnectionDirection direction) {
    return queue_map[type][direction].size();
}

