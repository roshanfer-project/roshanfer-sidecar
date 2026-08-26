Request Limit Protocol (RLP) is a protocol that ensures services only receive requests up to their configured limit.

# Message Types

1. Credit Request: Used to inform a server that the client has a number of requests to send. An important field is "requested credits". Note that this is only sent for new arriving requests to the client (for now it's always 1). There are no retries in the protocol. No timeouts are required (in fact not implemented in the current version) if we assume servers don't crash, which is out of our scope.
2. Credit Grant: It uses a the same header and structure as Credit Request. It is distinguished from Credit Request by setting a field. An important field is "granted credits". This type is the explciit response to a Credit Request. In other words, there is a 1-to-1 mapping between Credit Request and Credit Grant. **NOTE: In the new version of the protocol, we do not send back Credit Grants with no granted credits.** This message only goes back when there is a credit.
3. Credit return (`data[1] == 0x02`): Sent by a **client** to release a granted slot without forwarding an HTTP request (e.g. dfanout unused branches, or **ingress** when **`ingress_post_credit`** has no RPC to forward—**`0x02`** path—or legacy/defensive empty-dequeue handling). Handled by **`queue_multiplexer`** → **`decrement_in_flight(service)`**.

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

RLP is a stateful protocol, so both client and server rely on some state variables.

## Client-side

1. **PPMQueue** (non-ingress mesh nodes): Holds RPCs that already triggered a Credit Request and are waiting for credit before `forward_request`.

2. **Ingress-only**: There is **no `PPMQueue`** step for externally originated HTTP requests on the normal ingress HTTP path—pending RPCs live only in **`Ingress`’s deque**, and **`rpc_queue` EGRESS downstream** stays empty so **`ppm_client(false)`** does not stash ingress HTTP into **`PPMQueue`**. At most **one Credit Request is outstanding** per ingress worker (`Ingress::send_credit_request_checker` / `has_credit_request_on_fly`). Additional arrivals enqueue behind the head but **do not send another Credit Request** until the current grant cycle finishes (`ingress_post_credit` → **`send_sub_request`** or **credit return** → **`ingress_pre_credit`**). **`ppm_client(true)` is not used on ingress**; **`ingress_post_credit`** handles grants.

## Server-side

1. in_flight (bad name by the way!): This number keeps track of total active requests in the sidecar.
2. in_flight_per_endpoint: This is a map of endpoint to the number of active requests for that endpoint.
3. credit_queue: This is a queue of all Credit Requests that the sidecar has recieved and **haven't been able to credit immediately**. It helps the sidecar to keep track of rejected Credit Requests in case it wants to send a credit when a slot becomes available. There are two ways a credit can become available, which are the two ways active requests are decreased.

# Logic

## Client-side

### Ingress

1. After TCP READ handling, **`ingress_pre_credit`** may **`send_credit_request`** once per “credit cycle” when backlog exists and no Credit Request is in flight.
2. **`Ingress::enqueue`**: **Admission** is gated by **`ingress_size_cap`** ( **`drop_rpc` / 503** when the deque would exceed the cap). Adds **`rpc-id`** and **`priority`** headers. Tracks occupancy via **`ingress_mean`** (**`up`** on admit, **`down`** on **`dequeue`**) for AIMD (see **`rpc_flow.md`**, subsection **AIMD cap**).
3. On UDP Credit Grant, **`ingress_post_credit`** (exactly **one** credit): **`Ingress::dequeue`** pops the head RPC ( **`ingress_mean.down()`** ) → **`send_sub_request`** when present; otherwise **`prepare_credit_return`** / **`0x02`**. Then **`forward` EGRESS UPSTREAM** flushes 503s; then **`ingress_pre_credit`** (no **`PPMQueue`**).

### Every request (non-ingress clients)

1. At some point `State::ppm_client(false, …)` runs (`rpc_flow.md`).
2. New RPCs are taken from **`rpc_queue`** EGRESS downstream and pushed to **`PPMQueue`**.
3. A **Credit Request is sent per RPC** admitted that way (no batching).

### On every Credit Grant (non-ingress clients)

1. `State::ppm_client(true, …)` runs.
2. Pop **`PPMQueue`** by id / fan-out rules and **`send_sub_request`**.
3. Decrement **`in_flight`** where applicable (fan-out nuances unchanged).

> Parallel fan-out (`pfanout`): credit **`in_flight`** decrement only on the **last** branch (existing behavior).

### On responses

Clients receive responses on their **EGRESS** side and bump **`in_flight`** / accounting as before.

> Parallel fan-out: increment only on the **first** branch (existing behavior).

## Server-side

### On every Credit Request

1. `State::queue_multiplexer` gets called.
2. Checks if the in_flight is less than global limit and in_flight_per_endpoint is less than per_endpoint_limit.
3. If the in_flight is less than limit, the grant it and increment in_flight and in_flight_per_endpoint. Otherwise, store the rejected Credit Request info in a credit queue (shared among all threads).

### When a credit becomes available

1. Increment in_flight and in_flight_per_endpoint by 1.
2. We pop from the credit queue and send a Credit Grant.

# Note about sidecar roles

In every deployment, we definitely have one sidecar as Ingress, one as Frontend. Rest of the sidecars are neither ingress nor frontend.

A simple topology is shown below:

External clients --HTTP/1 (without RLP)--> Ingress --HTTP/1 (with RLP)--> Frontend --HTTP/2 (with RLP)--> Backend 1 --HTTP/2 (with RLP)--> ...

**Role-Specific Logic:**

- **Ingress**: 
  - **Drops**: **Head-drop when the ingress deque reaches `ingress_size_cap`** (**`Ingress::drop_rpc`** → 503). Other roles do not drop.
  - **Credits**: Ingress uses **RLP Credit Requests and grants** toward the frontend but buffers pending HTTP RPCs in **`Ingress::queue`**, not **`PPMQueue`**. It may send **`0x02` credit return** when a grant arrives but **`dequeue`** yields no RPC (**`ingress_post_credit`** path). Frontend **`queue_multiplexer`** treats **`0x02`** like other clients (**`decrement_in_flight`**).
- **Mesh Services**: Strictly enforce RLP credits and do not drop requests (at all).
