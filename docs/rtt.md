# Request Limit Protocol (RLP) / UDP RTT measurement

All times are **microseconds** (`steady_clock`). The RLP header is **26 bytes** plus the service name; byte `[2]` is `0x00` for a Credit Request and `0x01` for a Credit Grant.

## Client → server (Credit Request)

- **Bytes 14–21:** Send time when the client builds the Credit Request (`send_credit_request`). Same clock is used when the Credit Grant is received.
- **Bytes 22–25:** Piggyback `last_rtt_us` from the client’s **previous** credit measurement (signed, big-endian). The **server** reads this in `protocol_server` for `update_limits`; it is **not** queue dwell.

## Server Credit Grant

1. `protocol_server` builds a Credit Grant (copy of the request), sets `Buffer::enter_queue_ts`, then pushes it on `CreditQueue`.
2. When the Credit Grant is actually sent, `check_credit_transmission` sets **bytes 22–25** to **credit-queue dwell**: `now − enter_queue_ts` (time spent in `CreditQueue` only). This **replaces** the copied piggyback from the request on the wire.
3. Bytes **14–21** stay the **client’s** send stamp from the original Credit Request (echoed by the copy).

## Client (`protocol_client`, on credit receive)

- **Total elapsed:** `now_us −` bytes **14–21** (client-local round-trip until the Credit Grant is processed).
- **Queue dwell:** bytes **22–25** (unsigned decode); written by the server on the **Credit Grant** only.
- **Stored RTT:** `total − queueing_time` (approx. total time minus server credit-queue wait). Used for `last_rtt_us` and the next Credit Request’s piggyback.

## Limits

`update_limits` still uses the piggyback on **incoming Credit Requests** (bytes 22–25 on **requests**), then applies its own scaling; that path is independent of the dwell field on **Credit Grants**.
