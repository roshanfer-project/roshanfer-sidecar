# Load Balancing

## Terminology

- **Ingress:**: Sidecar at the gateway, which used to do overload control.
- **Mesh:**: All other sidecars.
- **Early-binding**: There is a 1:1 mapping between RPC and DN/credit. A credit can only be used for the RPC which issued the corresponding DN.
- **Late-binding**: A credit can be used for any RPC which is pending for credit. The mapping is made after the credit arrives, hence **late**.

## Setup

- Each microservice has multiple replicas.
- There can be multiple connections to a single replica (required for HTTP/1).


## Design

- Two-level selection: To get a connection, we must first **load balance** between replicas (to get a `ReplicaPool`) and then choose a connection available for that replica.
- The load balancing policy is based on least-loaded policy with load being the number of RPCs awaiting for credit.
- Ingress uses late-binding, but mesh is currently early-binding
- Ingress issues one DN per request in its queue **concurrently**

## Details
### Two-level selection

1. **Replica:** `ConnectionPool::lb()` picks the replica with the minimum waiting count.
2. **Connection (within replica):** `ReplicaPool::get_any_conn()` round-robins over `available()` connections.
   - HTTP/2 (mesh backends): one connection per replica; multiplexed.
   - HTTP/1.1: multiple connections per replica; same idle/available logic as before.

LB logic lives in the connection pool (when asking for a connection/replica).

### Waiting count tracker

- One `KeyValueMinTracker` (`waitings`) per `ConnectionPool` (i.e. per service), alongside a `bindings` map (`RPCID -> {lb_fd, replica_index}`).
- `increase(replica_index)` on DN send (`acquire`); `decrease(replica_index)` when the credit is resolved (`release`).
- Load counted = outstanding DNs (credits awaited) per replica; used by `lb()` to pick the least-loaded replica.
- All replicas initialized to 0 at startup via `init(index)` in `add_replica`.

### Binding lifecycle

A binding is created per DN and destroyed once its credit is resolved. Everything is keyed by `RPCID`, unique within a pool (dfanout sends one DN per downstream with the same `RPCID`, but into different pools, so keys never collide).

- `acquire(id)` — on DN send: `lb()` picks the replica, store the binding, `increase`.
- `peek(id)` — on routing: return the bound connection, **no mutation**.
- `release(id)` — on route success or credit return: `decrease` + erase the binding.

`route_request(type, ds_stream_id, ds_fd, credit_id)` looks up the binding by `credit_id`:

- **Early-binding (mesh):** `credit_id == rpc->get_local_id()` — the credit is spent on its own RPC.
- **Late-binding (ingress):** `credit_id` is the arriving credit's id, but the RPC served is the ingress queue head. Switching mesh to late-binding later is just passing a different `credit_id`.

It does `peek` → status check → `release` (EGRESS/committed path only), so a DOWN/TEARDOWN early-out leaves `waitings`/`bindings` intact for a retry.

### Tie-breaking

When multiple replicas have the same waiting count, the one with the **lowest replica index** is chosen (`std::set` ordering). Deterministic, but not round-robin among ties.

### dfanout

- **All downstreams:** `get_pool(ds_service).acquire(id)` → a binding in each downstream's own pool + a DN sent to it.
- **Primary (`dfanout_service`):** the credit is spent — `route_request` (`peek`+`release`) forwards the sub-request.
- **Others:** credit returned via `0x02`; the binding is released during the credit-return flush (`get_pool(*ret->ret_service).release(ret->ret_id)`), after all credits have arrived.

### Assumptions

- `State` is thread-local; the event loop processes one RPC step at a time per thread.
- Mesh path has no request drops (drops are ingress-only).
- Ingress may have multiple outstanding DNs; each has its own binding keyed by `RPCID`.

### Performance

- `KeyValueMinTracker` does O(log R) per update and O(1) min lookup (R = replica count, typically small).
- `bindings` is O(1) per `acquire`/`peek`/`release`. No heap allocations on the LB hot path.

## Call path

**Ingress:** `ingress_pre_credit` → `ConnectionPool::acquire(id)` (bind + waiting++) → `send_dn` → *(credit reply)* → `ingress_post_credit` → `Ingress::dequeue` (head) → `send_sub_request(head, credit_id)` → `route_request` (`peek`→`release`) → `forward_request`. Empty queue → return credit (`0x02`) + `release(credit_id)`.

**Mesh:** `ppm_client(false)` → `acquire(id)` per downstream → `send_dn` → *(credit reply)* → `ppm_client(true)` → `fanout_req_management` → served downstream forwarded via `send_sub_request` → `route_request` (`peek`→`release`); returned credits `release`d on flush.

## Key code

`ConnectionPool::acquire` / `peek` / `release` / `lb`, `ReplicaPool::get_any_conn`, `KeyValueMinTracker`, `PPMQueue::push` / `pop`.

Mesh: `State::ppm_client`, `State::fanout_req_management`, `State::send_sub_request`, `State::route_request`.

Ingress: `State::ingress_pre_credit`, `State::ingress_post_credit`, `State::send_sub_request`, `State::route_request`.
