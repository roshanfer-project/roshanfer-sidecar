PPM is a protocol that ensures services only receive requests up to their configured limit.

# Message Types

1. Demand Notification (DN): Used to inform a server that the client has a number of requests to send. An important field is "requested credits". Note that this is only sent for new arriving requests to the client (for now it's always 1). There are no retries in the protocol. No timeouts are required (in fact not implemented in the current version) if we assume servers don't crash, which is out of our scope.
2. Credit response: It uses a the same header and structure as DN. It is distinguished from DN by setting a field. An important field is "granted credits". This type is the explciit response to a DN. In other words, there is a 1-to-1 mapping between DN and credit response. **NOTE: In the new version of the protocol, we do not send back credit responses with no granted credits.** This message only goes back when there is a credit.

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

1. PPMQueue: It holds requests that should be sent to server using the PPM protocol. Requests in this queue have already made their pressence known to their desired services with a DN.

## Server-side

1. in_flight (bad name by the way!): This number keeps track of total active requests in the sidecar.
2. in_flight_per_endpoint: This is a map of endpoint to the number of active requests for that endpoint.
3. credit_queue: This is a queue of all DN requests that the sidecar has recieved and **haven't been able to credit immediately**. It helps the sidecar to keep track of rejected DNs in case it wants to send a credit when a slot becomes available. There are two ways a credit can become available, which are the two ways active requests are decreased.

# Logic

## Client-side

### Every request

1. At some point `State::ppm_client` is called (See `rpc_flow.md` for details).
2. New RPCs are added to PPMQueue.
3. A DN is sent for every new RPC (no batching).

### On every credit reply

1. At some point `State::ppm_client` is called (See `rpc_flow.md` for details).
2. Dequeue an RPC from PPMQueue and send it.
3. Decrement in_flight and in_flight_per_endpoint by 1.

> Note that if we have parallel fan-out (determined by `pfanout` in mapping configs), we do step 3 only when we receive credit for the **last** branch of fan-out

### On responses

Clients receive responses on their EGRESS-side. Therefore, they increment in_flight and in_flight_per_endpoint by 1.

> Note that if we have parallel fan-out, we only do this first the first branch.

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
  - **Drops**: Dropping Logic (`local_state.drops` and associated checks) **only happens at the Ingress sidecar**. Downstream sidecars (Frontend/Backend) do not drop requests (at all).
  - **Credits**: The Ingress sidecar (receiving external traffic) does **not** use the `in_flight` credit accounting mechanism for admission control. Credits are strictly for the internal PPM protocol between mesh services.
- **Mesh Services**: Strictly enforce PPM credits and do not drop requests (at all).

