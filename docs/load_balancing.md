# Load Balancing

## Scope

- **Mesh:** frontend and backend sidecars (`ppm_client` path).
- **Ingress:** ingress sidecar → frontend pool (sidecar-lb replication).

Both paths share `ConnectionPool::lb` and `KeyValueMinTracker`, but differ in **when** replica/connection binding is stored.

## Goal

Each microservice can have multiple replicas (DNS A-records → one `ReplicaPool` per address). Pick the replica with the fewest RPCs **waiting for PPM credit** — DN sent, credit not yet consumed.

## Two-level selection

1. **Replica:** `ConnectionPool::lb(KeyValueMinTracker*)` picks the replica with the minimum waiting count.
2. **Connection (within replica):** `ReplicaPool::get_any_conn()` round-robins over `available()` connections.
   - HTTP/2 (mesh backends): one connection per replica; multiplexed.
   - HTTP/1.1: multiple connections per replica; same idle/available logic as before.

LB logic lives in the connection pool (when asking for a connection/replica).

## Sticky binding at DN time (mesh)

When we send the DN, bind the request to its replica and connection. After receiving credit, do not re-check the connection pool.

- `State::do_lb(rpc)` runs before `send_dn` and `ppm_queue.push`.
- Sets `rpc->lb_replica_index` and `rpc->lb_fd`.
- After credit grant, `route_request(EGRESS)` reuses these fields.

## Late binding at Ingress

Ingress HTTP requests do not go through `PPMQueue`. They sit in `Ingress::queue` until credit arrives; at most **one DN is in flight** per worker. The DN is sent for `queue.front()`'s id **before** that RPC is dequeued (`ingress_pre_credit` → credit → `ingress_post_credit` → `Ingress::dequeue` → `send_sub_request`).

We cannot stick `lb_*` on the `RPCMessage` at DN time the same way as mesh: the mesh path binds on the RPC, then pushes into `PPMQueue`. Ingress keeps the RPC in its deque across the DN/credit gap.

**Late binding:** pick replica and connection at DN time, store on transient **`Ingress::lb_replica_index`** / **`Ingress::lb_fd`**, consume on forward.

1. **`ingress_pre_credit`:** `send_dn(do_lb(), ingress_service, …)` — `do_lb()` with no RPC selects a replica (using `ingress_service` and the waiting-count tracker), stores binding on `ingress.lb_*`, returns the UDP DN destination.
2. **`ingress_post_credit`:** `Ingress::dequeue` → `send_sub_request` → `route_request(EGRESS)`.
3. **`route_request` (ingress):** reads `ingress.lb_fd` / `ingress.lb_replica_index`, clears them to -1, opens the same TCP connection used for the DN.

Only ingress may call `do_lb()` without an RPC.

See [rpc_flow.md](rpc_flow.md) for the full ingress admission/credit cycle.

## Waiting count tracker

- `PPMQueue::replica_waiting_count[service]` — one `KeyValueMinTracker` per routed service.
- **Mesh:** `push` → `increase(replica_index)`; `pop` → `decrease(replica_index)`.
- **Ingress:** uses the same tracker for replica selection in `do_lb()`, but RPCs waiting in `Ingress::queue` are not yet reflected in the count (no `PPMQueue::push`). Counts only include mesh-queued RPCs for that service until separate ingress hooks are added.
- All replicas initialized to 0 at startup via `init(index)`.

## Tie-breaking

When multiple replicas have the same waiting count, the one with the **lowest replica index** is chosen (`std::set` ordering). Deterministic, but not round-robin among ties.

## dfanout

- **Primary downstream:** full `do_lb(rpc)` + sticky bind + `push`.
- **Other downstreams:** `lb(min_tracker)->get_addr()` for DN destination only; credit returned via `0x02` without HTTP forward. No `lb_*` stored on the RPC — we only need a replica address for PPM credit, not a connection binding.

## Assumptions

- `State` is thread-local; the event loop processes one RPC step at a time per thread.
- Mesh path has no request drops (drops are ingress-only).
- Ingress: one outstanding DN per worker → one `ingress.lb_*` slot suffices.

## Performance

`KeyValueMinTracker` does O(log R) per update and O(1) min lookup (R = replica count, typically small). No heap allocations on the LB hot path.

## Key code

Mesh: `State::do_lb(rpc)`, `ConnectionPool::lb`, `ReplicaPool::get_any_conn`, `PPMQueue::push` / `pop`, `KeyValueMinTracker`.

Ingress: `State::ingress_pre_credit`, `State::do_lb()` (no RPC), `Ingress::lb_*`, `State::ingress_post_credit`, `State::send_sub_request`, `State::route_request`.
