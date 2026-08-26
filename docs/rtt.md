# Request Limit Protocol (RLP) / UDP RTT measurement

All times are **microseconds** (`steady_clock`). The RLP header is **26 bytes** plus the service name; byte `[2]` is `0x00` for a DN **request** and `0x01` for a credit **response**.

## Client → server (DN request)

- **Bytes 14–21:** Send time when the client builds the DN (`send_dn`). Same clock is used when the credit reply is received.
- **Bytes 22–25:** Piggyback `last_rtt_us` from the client’s **previous** credit measurement (signed, big-endian). The **server** reads this in `queue_multiplexer` for `update_limits`; it is **not** queue dwell.

## Server credit reply

1. `queue_multiplexer` builds a response (copy of the request), sets `Buffer::enter_queue_ts`, then pushes it on `CreditQueue`.
2. When the reply is actually sent, `check_credit_transmission` sets **bytes 22–25** to **credit-queue dwell**: `now − enter_queue_ts` (time spent in `CreditQueue` only). This **replaces** the copied piggyback from the request on the wire.
3. Bytes **14–21** stay the **client’s** send stamp from the original DN (echoed by the copy).

## Client (`ppm_client`, on credit receive)

- **Total elapsed:** `now_us −` bytes **14–21** (client-local round-trip until the reply is processed).
- **Queue dwell:** bytes **22–25** (unsigned decode); written by the server on the **response** only.
- **Stored RTT:** `total − queueing_time` (approx. total time minus server credit-queue wait). Used for `last_rtt_us` and the next DN’s piggyback.

## Limits

`update_limits` still uses the piggyback on **incoming DNs** (bytes 22–25 on **requests**), then applies its own scaling; that path is independent of the dwell field on **responses**.
