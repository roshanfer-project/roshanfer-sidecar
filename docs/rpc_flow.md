# Request Flow

# Ingress

We use `ConnectionType::EGRESS` for mesh-facing TCP because it matches Request Limit Protocol (RLP) routing: requests arrive on the listener typed `INGRESS`, but each parsed RPC is handled as **EGRESS downstream** toward the frontend pool. Responses follow the reverse path. **`ConnectionType::INGRESS` TCP path is not used for ingress HTTP.** Only HTTP/1.

## Request

Requests are received on `ConnectionDirection::DOWNSTREAM` on the EGRESS listener.

1. Event loop handles a `READ` completion.
2. `HTTPConnection::http_read` parses the request and allocates the `RPCMessage`.
3. For ingress, the RPC is **`Ingress::enqueue`** — not admitted into `RPCQueue` for mesh forwarding.

### Ingress queue (single buffer before mesh)

4. **`Ingress::enqueue`** (`ingress.cc`):
   - **Admission:** if **`queue.size() >= ingress_size_cap`**, **`drop_rpc`** (503 on **`rpc_queue` EGRESS UPSTREAM**) and return—strict cap (**admit only when `size < ingress_size_cap`**).
   - Otherwise adds **`rpc-id`** and **`priority`** headers (`add_rpc_id_header` / `add_priority_header`), bumps **`ingress_mean.up()`** (time-weighted occupancy tracker), **`push_back`** onto the deque.
   - Ingress requires **`routing.slo`** and **`routing.priority`** for the service (validated in ctor).

5. Event loop calls **`State::forward`** (drops/responses only on usual queues).

6. **`State::ingress_pre_credit`** (`state.cc`):
   - If **`stats.tail_e2e_time_us`** reported a fresh smoothed quantile flush (**`consume_flush_updated()`**), **`Ingress::update_ingress_cap()`** runs (**AIMD** on **`ingress_size_cap`**; see below).
   - **`Ingress::send_dn_checker`**: if the deque is non-empty and there is **no DN already in flight** (`has_dn_on_fly`), sets `has_dn_on_fly` and returns true.
   - If true: **`send_dn`** with **`queue.front()`**’s `get_id()` / `get_priority()` (same ids as HTTP headers).

7. **`State::ppm_client(false, nullptr)`** runs afterward; for ingress it only drains **`rpc_queue` EGRESS downstream** if anything was queued there (ingress mesh requests **do not** go through that queue). Frontend/backend still use this path.

### Credit grant → forward

8. On UDP **credit grant** (`dispatch_rlp_recv`, response to our DN), ingress calls **`State::ingress_post_credit`** (not `ppm_client(true, …)`).

9. **`ingress_post_credit`**: **`credit_post_process`**, then asserts **`num_credits == 1`** (batched grants are fatal on ingress).

10. **`Ingress::dequeue`** pops the **front** RPC, clears **`has_dn_on_fly`**, **`ingress_mean.down()`**, returns **`std::optional`** carrying that RPC. (**Implementation detail:** today an empty deque is **`LOG(FATAL)`**; **`ingress_post_credit`** still has a branch that emits **`0x02`** if dequeue returns **`nullopt`**—useful if the implementation later allows an empty deque under a race.)

11. If **`dequeue`** returned a value → **`send_sub_request`** (`route_request` + **`forward_request`**, no **`PPMQueue`**).

12. If **`dequeue`** returned **nullopt** → ingress sends a **credit return** (`prepare_credit_return` on the grant buffer → **`data[1]=0x02`**) so the frontend frees **`in_flight`**.

13. **`forward(ConnectionType::EGRESS, ConnectionDirection::UPSTREAM)`** drains queued **503** responses from **`drop_rpc`** so clients do not wait for an unrelated TCP READ.

14. **`ingress_pre_credit`** issues the **next DN** if there is backlog and **`send_dn_checker`** allows it.

15. Elsewhere, **`write_http`** flushes bytes on connections.

### AIMD cap (`ingress_size_cap`)

Completed RPCs update **`Stats::tail_e2e_time_us`** (**`SmoothedQuantileEstimator`**: empirical p99 over batched samples, EMA-smoothed; default batch **100** samples or **200 ms**, \(\alpha=0.7\)). Each time that estimator finishes a flush that updates the smoothed value, **`ingress_pre_credit`** runs **`Ingress::update_ingress_cap()`**.

Rough control law (constants in **`ingress.h`**): **`err = (ema_us − slo_us) / slo_us`** where **`slo_us = slo_ms * 1000`** matches microsecond latency samples. If **`err > aimd_err_d`**, **`ingress_size_cap`** is multiplicatively decreased using **`ceil(cap / aimd_adj_d)`** (less aggressive than truncating down). If **`err < aimd_err_i`**, the cap may increase when the time-average ingress occupancy (**`ingress_mean.value()`** since the last read) is high relative to the cap, or decrease slightly when the cap exceeds **`safe_multiply *`** time-average downstream concurrency (**`time_mean_ds_concurrency`**); that branch also uses **`ceil(cap / aimd_adj_d)`** for the lowered candidate. Finally **`ingress_size_cap ≥ 1`**.

**Signals:** **`ingress_mean`**—**`up`** on admit, **`down`** on **`dequeue`**—feeds average queue depth between AIMD reads. **`time_mean_ds_concurrency`**—**`up`** on **`send_sub_request`**, **`down`** on egress response handling—approximates work visible past ingress for the concurrency guard.

### Dropped request (ingress only)

Ingress drops are **`Ingress::drop_rpc`**: **head-drop when the ingress deque has reached `ingress_size_cap`**, and the same helper for explicit error responses. Drops enqueue **`RPCQueue` EGRESS UPSTREAM** and **`forward`** drains **503**s. Downstream sidecars do not run ingress drop logic.

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

3. Same RLP client pattern as other mesh nodes: **`ppm_client(false)`** drains **`rpc_queue` → `PPMQueue` → `send_dn`**; **`ppm_client(true)`** on grant pops **`PPMQueue`** and forwards. **Unlike ingress**, the frontend keeps admitted mesh RPCs in **`PPMQueue`** until credited.

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
