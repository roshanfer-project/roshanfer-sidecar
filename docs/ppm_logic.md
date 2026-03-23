PPM is a protocol that ensures services only receive requests up to their configured limit.

# Message Types

1. Demand Notification (DN): Used to inform a server that the client has a number of requests to send. An important field is "requested credits". Note that this is only sent for new arriving requests to the client (for now it's always 1). There are no retries in the protocol. No timeouts are required (in fact not implemented in the current version) if we assume servers don't crash, which is out of our scope.
2. Credit response: It uses a the same header and structure as DN. It is distinguished from DN by setting a field. An important field is "granted credits". This type is the explicit response to a DN. In other words, there is a 1-to-1 mapping between DN and credit response. **NOTE: In the new version of the protocol, we do not send back credit responses with no granted credits.** This message only goes back when there is a credit.

# Parameters

On mesh (non-ingress) sidecars:


1. **Single admission cap (`ppm_limit`):** The server uses **one** number for admission: the **maximum** of all per-endpoint limits. There is no extra “sum of locals” globals. We use the maximum per endpoint limit as this global limit.
2. **`over_commitment` (optional YAML):** If set (0–1), it **scales up** the RTT-derived per-endpoint limit before `extra_limit` is added.

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

1. PPMQueue: It holds requests that should be sent to server using the PPM protocol. Requests in this queue have already made their presence known to their desired services with a DN.

## Server-side

1. **in_flight** (name is a bit misleading): Tracks how many requests are **active in the local service** for credit accounting (see “Active requests” above).
2. **credit_queue:** Every DN gets a prepared credit reply and is **queued** here first. The sidecar sends that reply only when admission rules allow (see below). So there is no separate “instant grant vs reject” path: sending is always deferred through this queue and `check_credit_transmission`.

# Logic

## Client-side

### Every READ event

1. At some point `State::ppm_client` is called (See `rpc_flow.md` for details).
2. New RPCs are added to PPMQueue.
3. A DN is sent for every new RPC (no batching).

### On every credit reply

1. At some point `State::ppm_client` is called (See `rpc_flow.md` for details).
2. Dequeue an RPC from PPMQueue and send it.

## Server-side

### On every DN

1. `State::queue_multiplexer` runs: it builds the credit reply and **pushes** it into `credit_queue` (shared across threads on that process).
2. It then tries to flush credits via `check_credit_transmission` → `CreditQueue::pop`.

### When a credit may be sent (`CreditQueue::pop`)

A queued credit is actually sent only if **all** of these hold:

1. `in_flight < ppm_limit` (room under the single cap).
2. For that endpoint, the **endpoints’s** `PPMQueue` in the sidecar is **empty**. That way we do not hand out credits while end endpoint is blocked by its downstreams (backpressure).

When a reply is sent, `in_flight` is increased as part of granting. Slots free up again when active work drops, same as before (see “Active requests”).

**Threading:** Non-ingress configs are expected to use **one** event-loop thread per process so this `PPMQueue` check matches reality. Multi-thread mesh would need a different way to see queued depth.

# Note about sidecar roles

In every deployment, we definitely have one sidecar as Ingress, one as Frontend. Rest of the sidecars are neither ingress nor frontend.

A simple topology is shown below:

External clients --HTTP/1 (without PPM protocol)--> Ingress --HTTP/1 (with PPM protocol)--> Frontend --HTTP/2 (with PPM protocol)--> Backend 1 --HTTP/2 (with PPM protocol)--> ...

**Role-Specific Logic:**

- **Ingress**: 
  - **Drops**: Dropping Logic (`local_state.drops` and associated checks) **only happens at the Ingress sidecar**. Downstream sidecars (Frontend/Backend) do not drop requests (at all).
  - **Credits**: The Ingress sidecar (receiving external traffic) does **not** use the `in_flight` credit accounting mechanism for admission control. Credits are strictly for the internal PPM protocol between mesh services.
- **Mesh Services**: Strictly enforce PPM credits and do not drop requests (at all).

