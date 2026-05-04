# Request Flow

# Ingress

We use `ConnectionType::EGRESS` for mesh-facing TCP because it matches PPM routing: requests arrive on the listener typed `INGRESS`, but each parsed RPC is handled as **EGRESS downstream** toward the frontend pool. Responses follow the reverse path. **`ConnectionType::INGRESS` TCP path is not used for ingress HTTP.** Only HTTP/1.

## Request

Requests are received on `ConnectionDirection::DOWNSTREAM` on the EGRESS listener.

1. Event loop handles a `READ` completion.
2. `HTTPConnection::http_read` parses the request and allocates the `RPCMessage`.
3. For ingress, the RPC is **`Ingress::enqueue`** — not admitted into `RPCQueue` for mesh forwarding.

### Ingress queue (single buffer before mesh)

4. **`Ingress::enqueue`** (`ingress.cc`):
   - Computes a **deadline** (microseconds): `slo_us = routing.slo_ms * 1000`, then `slack = slo_us - ceil(stats.tail_ds_service_time_us)` for this service. If **`slack < 0`** → **`LOG(FATAL)`**. Then a **5% guard** on the SLO budget: **`slack -= (int)(slo_us * 0.05)`** (same intent as keeping ~95% of the budget after subtracting tail latency).
   - Sets `rpc->deadline = now + slack`.
   - Adds **`rpc-id`** and **`priority`** headers (`add_rpc_id_header` / `add_priority_header`), which also populate `RPCMessage::id` and `::priority` for HTTP (`rpc_message.cc`).
   - **`push_back`** onto `Ingress`’s FIFO deque.

5. Event loop calls **`State::forward`** (drops/responses only on usual queues).

6. **`State::ingress_pre_credit`** (`state.cc`):
   - **`Ingress::send_dn_checker`**: if the deque is non-empty and there is **no DN already in flight** (`has_dn_on_fly`), sets `has_dn_on_fly` and returns true.
   - If true: **`send_dn`** with **`queue.front()`**’s `get_id()` / `get_priority()` (same ids as HTTP headers).

7. **`State::ppm_client(false, nullptr)`** runs afterward; for ingress it only drains **`rpc_queue` EGRESS downstream** if anything was queued there (ingress mesh requests **do not** go through that queue). Frontend/backend still use this path.

### Credit grant → forward

8. On UDP **credit grant** (`dispatch_ppm_recv`, response to our DN), ingress calls **`State::ingress_post_credit`** (not `ppm_client(true, …)`).

9. **`ingress_post_credit`**: **`credit_post_process`**, then asserts **`num_credits == 1`** (batched grants are fatal on ingress).

10. **`Ingress::dequeue`** returns **`std::optional<RPCMessage>`**:
    - Walks from the **front**: expired RPCs → **`drop_rpc`** (503 on **`rpc_queue` EGRESS UPSTREAM**).
    - On first non-expired RPC → returns it.
    - If **every** RPC expired → returns **`std::nullopt`** (deque empty).
    - **`has_dn_on_fly`** is cleared on **every** exit from **`dequeue`** (success or empty-after-drops).

11. If **`dequeue`** returned a value → **`send_sub_request`** (`route_request` + **`forward_request`**, no **`PPMQueue`**).

12. If **`dequeue`** returned **nullopt** → no mesh forwarder for this grant; ingress sends a **credit return** (`prepare_credit_return` on the grant buffer → **`data[1]=0x02`**) so the frontend frees **`in_flight`** (same wire shape as dfanout credit return).

13. **`forward(ConnectionType::EGRESS, ConnectionDirection::UPSTREAM)`** drains queued **503** responses from **`drop_rpc`** so clients do not wait for an unrelated TCP READ.

14. **`ingress_pre_credit`** issues the **next DN** if there is backlog and **`send_dn_checker`** allows it.

15. Elsewhere, **`write_http`** flushes bytes on connections.

### Dropped request (ingress only)

Ingress drops are **`Ingress::drop_rpc`** (deadline expired at **`dequeue`**) or future hooks; they enqueue **`RPCQueue` EGRESS UPSTREAM** and **`forward`** (above or later READ path) writes the 503. Downstream sidecars do not run ingress drop logic.

## Response

Same as before: upstream READ → parse → `RPCQueue` → `forward` → `write_http`.

---

# Frontend

For this sidecar, HTTP/1 for `ConnectionType::INGRESS` and gRPC for `ConnectionType::EGRESS`.

## Request

Direction `DOWNSTREAM`.

1. READ → `http_read` → RPC admitted into **`RPCQueue`** (HTTP/2 uses nghttp2 callbacks; see Backend).

### `ConnectionType::INGRESS` requests

2. `forward` → `route_request` → `forward_request` → `write_http`.

### `ConnectionType::EGRESS` requests

3. Same PPM client pattern as other mesh nodes: **`ppm_client(false)`** drains **`rpc_queue` → `PPMQueue` → `send_dn`**; **`ppm_client(true)`** on grant pops **`PPMQueue`** and forwards. **Unlike ingress**, the frontend keeps admitted mesh RPCs in **`PPMQueue`** until credited.

## Response

Same pattern as ingress responses above.

---

# Backend services

gRPC on both sides; same idea as frontend EGRESS path. See **`HTTP2Connection::http_read`** / nghttp2 callbacks.

## nghttp2 callbacks

1. `HTTP2Connection::http_read` drives callbacks.
2. `on_begin_headers_callback` allocates RPC.
3. `on_header_callback` fills headers.
4. `on_data_chunk_recv_callback` for DATA.
5. `frame_recv_callback` detects RPC completion → **`RPCQueue`**.
6. Same **`forward` / `ppm_client`** flow as frontend EGRESS.
