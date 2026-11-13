# RPC ids: `global_id`, `local_id`, and EGRESS → INGRESS mapping

## Headers and fields

| Header | Field on `RPCMessage` | Role |
|--------|------------------------|------|
| `rpc-id` | `global_id` | End-to-end correlation (debug, tracing) |
| `rpc-local-id` | `local_id` | 1. Distinguish downstream requests of a single upstream request. 2. Find upstream request from downstream requests. 3. This is the id used for Credit Request/Credit Grant. |

Both are carried as decimal strings in HTTP/gRPC metadata. `RPCID` is `int64_t`; `-1` means “unset” for `local_id` where explicitly initialized.

## Vocabulary
- Generating: Calculating an id.
- Setting: Serializing an id into HTTP/gRPC metadata. Note the difference from generating.
- Reading: Deserializing HTTP/gRPC metadata corresponding to an id.

- Ingress vs Mesh sidecars: Ingress is the gateway and Mesh is any other sidecar.

## `global_id`

It is generated once at the Ingress. It is propagated with `rpc-id` at both INGRESS and EGRESS sides throughout the mesh without any change.

## `local_id`

In Ingress it is the same as `global_id`. This is fine because there is no fan-out at Ingress.

For meth sidecars, it is generated at INGRESS-side and propagated to EGRESS-side with `rpc-local-id` header.
EGRESS-side reads it and uses it to generate EGRESS-side local id using the mechanism described below.

**`rpc-local-id` is only used to inform EGRESS-side about the id of the INGRESS-side. Thus, it is not set for EGRESS-requests.**

## Packing (`get_new_local_id`)

One 64-bit `local_id` holds:

- **High 32 bits:** `uint32_t(parent)` — for EGRESS-side mesh requests, `parent == P` from `rpc-local-id` (`P > 0`).
- **Low 32 bits:** `uint32_t(local_id_counter)` — monotonic branch suffix; counter increment is global; low lane is masked so it never ORs into the parent lane.

Decode (unsigned): `parent = (uint64_t)local_id >> 32`, `branch = (uint32_t)local_id`.

`RPCID` is signed; some packed values appear negative when printed—only **`local_id == -1`** is reserved as unset.

## Mapping EGRESS-side `local_id` → INGRESS RPC `local_id` (`get_ingress_rpc`)

`get_ingress_rpc(id)` is used on **EGRESS-side** `local_id` (PPM, fan-out counters, credit paths). It does **not** assume a separate side table:

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
