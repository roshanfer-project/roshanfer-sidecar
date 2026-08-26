# RPC ids: `global_id`, `local_id`, and EGRESS → INGRESS mapping

## Headers and fields

| Header | Field on `RPCMessage` | Role |
|--------|------------------------|------|
| `rpc-id` | `global_id` | End-to-end correlation (debug, tracing). Set from the wire on every hop. |
| `rpc-local-id` | `local_id` (after parsing rules below) | Per-sidecar routing, RLP, `id_map` / `ppm_queue` keys. |

Both are carried as decimal strings in HTTP/gRPC metadata. `RPCID` is `int64_t`; `-1` means “unset” for `local_id` where explicitly initialized.

## `global_id`

- Read from `rpc-id` on each request.
- **Ingress edge** (`config.is_ingress`): after `rpc-id`, `local_id` is initially set to **`global_id`** (same value) for the RPC admitted toward the local service.
- Elsewhere, `global_id` does not by itself define `local_id` on the EGRESS-facing path; it remains separate for logging and correlation.

## `local_id` on the INGRESS-style path (toward the hosted service)

“INGRESS” here means **connection type** `ConnectionType::INGRESS` (sidecar → local service), not necessarily `config.is_ingress`.

- On **`rpc-id`**, if not ingress edge: with `ConnectionType::INGRESS`, `local_id = get_new_local_id(0)` (see packing below with `parent == 0`). That value is what gets inserted into `id_map[ConnectionType::INGRESS]` when `route(…)` runs for that RPC—**that integer is the map key you must be able to recover from outbound EGRESS traffic** (see wire rule).
- **Ingress edge** HTTP path can also assign ids via `Ingress::add_rpc_id_header` (separate counter); those end up as `rpc-id` / `global_id` / `local_id` per `rpc_message.cc`.

## `local_id` on the EGRESS-style path (from service toward mesh)

Requests arrive on an **EGRESS** listener (`ConnectionType::EGRESS`). `rpc-id` does not assign a final `local_id` on that connection type; the mesh id is fixed when **`rpc-local-id`** is parsed:

- Numeric value `P` must satisfy **`1 ≤ P ≤ UINT32_MAX`** (`P == 0` is rejected: it would not encode a valid parent lane).
- Then `local_id = get_new_local_id(P)` (packed composite, see below).
    The header is **not** stored in the forwarded request list for downstream (early `return` in `add_header_field`): the sidecar stops propagating this hop’s raw `rpc-local-id` as a stored header field on EGRESS parses.

**Wire rule:** On mesh, treat **`rpc-local-id` as the parent key `P`** the downstream sidecar should use—**not** a full 64-bit composite from a previous hop, unless you change the parser (values `> UINT32_MAX` are rejected).

## Packing (`get_new_local_id`)

One 64-bit `local_id` holds:

- **High 32 bits:** `uint32_t(parent)` — for EGRESS mesh traffic, `parent == P` from `rpc-local-id` (`P > 0`).
- **Low 32 bits:** `uint32_t(local_id_counter)` — monotonic branch suffix; counter increment is global; low lane is masked so it never ORs into the parent lane.

Decode (unsigned): `parent = (uint64_t)local_id >> 32`, `branch = (uint32_t)local_id`.

`RPCID` is signed; some packed values appear negative when printed—only **`local_id == -1`** is reserved as unset.

## Mapping EGRESS `local_id` → INGRESS RPC (`get_ingress_rpc`)

`get_ingress_rpc(id)` is used for **EGRESS** composite ids (RLP, fan-out counters, credit paths). It does **not** assume a separate side table:

1. `ingress_side_id = (uint64_t)id >> 32`.
2. If `ingress_side_id == 0` → **`LOG(FATAL)`** (invalid on the EGRESS path: parent 0 is not allowed for mesh composites).
3. Else return `id_map[ConnectionType::INGRESS].at(ingress_side_id)`.

So the **high 32 bits** of the EGRESS `local_id` **must equal** the key under which the **ingress-side** RPC object was inserted in `id_map[INGRESS]` when that hop forwarded the request **into the local service**. The service (or shim) must echo a `rpc-local-id` value **`P`** equal to that key; parallel outbound calls each get their own low-half branch while sharing the same high-half `P`.

## Invariants (summary)

1. **Mesh EGRESS:** `rpc-local-id` → `P` with `1 ≤ P ≤ UINT32_MAX`; `local_id = get_new_local_id(P)`.
2. **Lookup:** EGRESS composite `id` → ingress row keyed by `(uint32_t)((uint64_t)id >> 32)`.
3. **Parent 0** is invalid for mesh composites; `get_ingress_rpc` enforces nonzero high half.
4. **Branch lane** wraps modulo \(2^{32}\); avoid relying on uniqueness across extreme lifetime without resetting or widening.

## Code pointers

- Packing and parse: [`src/rpc_message.cc`](../src/rpc_message.cc) (`get_new_local_id`, `add_header_field` for `rpc-id` / `rpc-local-id`).
- Ingress lookup: [`src/rpc_mapper.cc`](../src/rpc_mapper.cc) (`get_ingress_rpc`).
- Map insert on forward: [`src/rpc_mapper.cc`](../src/rpc_mapper.cc) (`route` after `forward_request`).
