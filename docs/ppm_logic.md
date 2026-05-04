PPM is a protocol that ensures services only receive requests up to their configured limit.

# Message Types

1. Demand Notification (DN): Used to inform a server that the client has a number of requests to send. An important field is "requested credits". Note that this is only sent for new arriving requests to the client (for now it's always 1). There are no retries in the protocol. No timeouts are required (in fact not implemented in the current version) if we assume servers don't crash, which is out of our scope.
2. Credit response: It uses a the same header and structure as DN. It is distinguished from DN by setting a field. An important field is "granted credits". This type is the explciit response to a DN. In other words, there is a 1-to-1 mapping between DN and credit response. **NOTE: In the new version of the protocol, we do not send back credit responses with no granted credits.** This message only goes back when there is a credit.
3. Credit return (`data[1] == 0x02`): Sent by a **client** to release a granted slot without forwarding an HTTP request (e.g. dfanout unused branches, or **ingress** when every queued RPC missed its deadline after receiving a grant). Handled by **`queue_multiplexer`** → **`decrement_in_flight(service)`**.

# Parameters

Non-ingress services have two parameters:

1. PPM Limit (or global limit): This is the maximum total number of requests a service can have **active** at any time.
2. Per-Endpoint Limit: This is a cap for each endpoint.

# What are **Active** requests?

A request is active if it is being processed (or at least we know its thread is not blocked) in the local service of the sidecar.
Active requests are increases when:

1. A request is received from the `INGRESS` side.
2. A response is received from the `EGRESS` side.

Active requests are decreases when:

1. A response is sent from the `INGRESS` side.
2. A request is sent from the `EGRESS` side (we assume synchronous RPCs, so the corresponding thread goes to sleep).

# Protocol State

PPM is a stateful protocol, so both client and server rely on some state variables.

## Client-side

1. **PPMQueue** (non-ingress mesh nodes): Holds RPCs that already triggered a DN and are waiting for credit before `forward_request`.

2. **Ingress-only**: There is **no `PPMQueue`** step for externally originated HTTP requests. Pending RPCs live only in **`Ingress`’s deque**. At most **one DN is outstanding** per ingress worker (`Ingress::send_dn_checker` / `has_dn_on_fly`). Additional arrivals enqueue behind the head but **do not send another DN** until the current grant cycle finishes (`ingress_post_credit` → optional **`send_sub_request`** or **credit return** → **`ingress_pre_credit`**). **`ppm_client(true)` is not used on ingress**; **`ingress_post_credit`** handles grants. If **all** queued RPCs violate their deadline when the grant arrives, ingress sends **`0x02` credit return** (no HTTP forward for that grant).

## Server-side

1. in_flight (bad name by the way!): This number keeps track of total active requests in the sidecar.
2. in_flight_per_endpoint: This is a map of endpoint to the number of active requests for that endpoint.
3. credit_queue: This is a queue of all DN requests that the sidecar has recieved and **haven't been able to credit immediately**. It helps the sidecar to keep track of rejected DNs in case it wants to send a credit when a slot becomes available. There are two ways a credit can become available, which are the two ways active requests are decreased.

# Logic

## Client-side

### Ingress

1. After TCP READ handling, **`ingress_pre_credit`** may **`send_dn`** once per “credit cycle” when backlog exists and no DN is in flight.
2. **`Ingress::enqueue`** assigns **`rpc-id`**, **`priority`**, and a **deadline**: **`slo_us = slo_ms * 1000`**, **`slack = slo_us - ceil(tail_ds_service_time_us)`** (fatal if negative), then **`slack -= (int)(slo_us * 0.05)`**; see `ingress.cc` and `rpc_flow.md`.
3. On UDP credit grant, **`ingress_post_credit`** (exactly **one** credit): **`Ingress::dequeue`** drops expired-at-front RPCs; returns **`nullopt`** if none survive → **`prepare_credit_return`** / **`0x02`** to frontend; otherwise **`send_sub_request`**. Then **`forward` EGRESS UPSTREAM** flushes 503s; then **`ingress_pre_credit`** (no **`PPMQueue`**).

### Every request (non-ingress clients)

1. At some point `State::ppm_client(false, …)` runs (`rpc_flow.md`).
2. New RPCs are taken from **`rpc_queue`** EGRESS downstream and pushed to **`PPMQueue`**.
3. A **DN is sent per RPC** admitted that way (no batching).

### On every credit reply (non-ingress clients)

1. `State::ppm_client(true, …)` runs.
2. Pop **`PPMQueue`** by id / fan-out rules and **`send_sub_request`**.
3. Decrement **`in_flight`** where applicable (fan-out nuances unchanged).

> Parallel fan-out (`pfanout`): credit **`in_flight`** decrement only on the **last** branch (existing behavior).

### On responses

Clients receive responses on their **EGRESS** side and bump **`in_flight`** / accounting as before.

> Parallel fan-out: increment only on the **first** branch (existing behavior).

## Server-side

### On every DN

1. `State::queue_multiplexer` gets called.
2. Checks if the in_flight is less than global limit and in_flight_per_endpoint is less than per_endpoint_limit.
3. If the in_flight is less than limit, the grant it and increment in_flight and in_flight_per_endpoint. Otherwise, store the rejected DN info in a credit queue (shared among all threads).

### When a credit becomes available

1. Increment in_flight and in_flight_per_endpoint by 1.
2. We pop from the credit queue and send a credit response.

# Note about sidecar roles

In every deployment, we definitely have one sidecar as Ingress, one as Frontend. Rest of the sidecars are neither ingress nor frontend.

A simple topology is shown below:

External clients --HTTP/1 (without PPM protocol)--> Ingress --HTTP/1 (with PPM protocol)--> Frontend --HTTP/2 (with PPM protocol)--> Backend 1 --HTTP/2 (with PPM protocol)--> ...

**Role-Specific Logic:**

- **Ingress**: 
  - **Drops**: Deadline-based shedding and explicit **`drop_rpc`** paths run **only** at ingress (503 to client). Other roles do not drop.
  - **Credits**: Ingress uses **PPM DNs and grants** toward the frontend but buffers pending HTTP RPCs in **`Ingress::queue`**, not **`PPMQueue`**. It may send **`0x02` credit return** when a grant arrives but **no** RPC is eligible (all deadlines expired). Frontend **`queue_multiplexer`** treats **`0x02`** like other clients (**`decrement_in_flight`**).
- **Mesh Services**: Strictly enforce PPM credits and do not drop requests (at all).

