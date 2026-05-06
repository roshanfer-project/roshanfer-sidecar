# Adding Headers to gRPC and HTTP Requests

When injecting a custom header into a forwarded request, the correctness requirements differ between HTTP/1 and HTTP/2 (gRPC).

## The core rule

Always pass the **actual string length**, not the buffer size, as the value length.

```cpp
// WRONG — passes full array size (e.g. 32), including trailing null bytes
add_header_field(NAME, NAME_LEN, buf.data(), buf.size(), ...);

// CORRECT — snprintf returns the number of characters written (excluding null)
size_t len = std::snprintf(buf.data(), buf.size(), "%lld", value);
add_header_field(NAME, NAME_LEN, buf.data(), len, ...);
```

Apply the same rule to the name length: do **not** include the null terminator.

```cpp
// WRONG
const size_t MY_HEADER_NAME_LEN = 13; // "my-header" is 9 chars, not 10

// CORRECT
const size_t MY_HEADER_NAME_LEN = 9;
```

## Why it matters for gRPC but not HTTP/1

| | HTTP/1 | HTTP/2 / gRPC |
|---|---|---|
| Header serialization | `%.*s` format — stops at `\0` | `nghttp2_nv.valuelen` — raw byte count, no null-stopping |
| Null bytes in value | Silently truncated | HPACK-encoded verbatim into the wire frame |
| Server behaviour on null bytes | Ignored | `RST_STREAM` (PROTOCOL_ERROR) |

For HTTP/1, `connection.cc` writes headers using:
```cpp
append_fmt_or_fatal(buffer, written, "%.*s: %.*s\r\n",
    (int)name_len, name.data(), (int)value_len, value.data());
```
`%.*s` stops at the first null byte, so oversized `value_len` is harmless.

For HTTP/2, `connection.cc` passes lengths directly to nghttp2:
```cpp
nva[i].valuelen = req_headers.at(i)->value_len; // used verbatim by HPACK encoder
```
nghttp2 encodes every byte up to `valuelen`, including null bytes. The receiving gRPC server (e.g. gRPC-go) validates HPACK header values and rejects frames containing null bytes with `RST_STREAM`.

## Adding a new header

1. Define the name as a string literal and set the length to `strlen` of that literal (no null terminator):
   ```cpp
   const uint8_t *MY_HEADER_NAME = reinterpret_cast<const uint8_t *>("x-my-header");
   const size_t MY_HEADER_NAME_LEN = 11; // strlen("x-my-header")
   ```

2. Format the value and capture the length from `snprintf`:
   ```cpp
   std::array<char, 32> buf;
   buf.fill(0);
   size_t len = std::snprintf(buf.data(), buf.size(), "%d", my_value);
   add_header_field(MY_HEADER_NAME, MY_HEADER_NAME_LEN,
                    reinterpret_cast<const uint8_t *>(buf.data()),
                    len, /*request=*/true, /*trailer=*/false);
   ```

3. Call `add_header_field` with `len`, not `buf.size()`.
