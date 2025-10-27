# Request Flow

# Ingress
We are using `ConnectionType::EGRESS` path because it fits better with `ppm` logics. Only HTTP/1 is used here.

## Request
Requests are recieved on `ConnectionDirection::DOWNSTREAM` direction.

1. Event loop, we receive the incoming traffic as a `READ` event.
2. Event loop calls `HTTPConnection::http_read` to parse the incoming data.
2.1. It allocates the `RPCMessage`.
2.2. It enqueues the `RPCMessage` in the `Ingress::Ingress`.
3. Event loop calls `State::ingress_admit` to admit or drop requests.

### Admitted request

4. Upon admission, `State::ingress_admit` adds the RPC to `RPCQueue`.
5. Later, event loop calls `State::ppm_client` that reads from `RPCQueue` and
5.1 Routes them (finding an apppropriate upstream connection for them) using `Satte::route_request`.
5.2 Adds the RPC to `PPMQueue`.
5.3 Sends a DN request for it.
6. When we receive a DN response, the event loop executes the `RCVMSG` operation by calling `State::ppm_client` again but this time setting the first argyment to `true`.
7. If a valid credit has been recieved, `State::ppm_client` pops from `PPMQueue` and calls`State::forward_request` that maps the downstream and upstream connections and stream ids can calls thw `State::write_http`.
8. `write_http` uses the `HTTPConnection::http_write` **virtual** method to write the rpc to the `iouring`.

### Dropped request
4. In `Ingress::check_drop` we prepare the request for dropping (e.g., setting the status code), map it to upstream connection (drop connection with `drop_fd` fd).
5. Add the RPC to `RPCQueue` of the upstream side.
6. Then, in `State::ingress_admit` we call `State::forward` that calls `State::write_http`.
5. `State::write_http` uses **virtual** `HTTPConnection::http_write` methods to write back the response.

## Response

1. In event loop, we receive the incoming traffic as a READ event.
2. We read the traffic in the corresponding connection (HTTP/1) using `HTTPConnection::http_read` virtual methods provided by connections (Executing `ConnectionDirection::UPSTREAM` routine).
3. `HTTPConnection::http_read` reads the response into the RPC and adds it to `RPCQueue`.
4. Later, event loop calls `State::forward` that calls `State::write_http`.
5. `State::write_http` uses virtual `HTTPConnection::http_write` methods to write back the response.


# Frontend
For this sidecar, we have HTTP/1 for `ConnectionType::INGRESS` but gRPC for `ConnectionType::EGRESS`.

## Request
For requests, we always have `ConnectionDirection::DOWNSTREAM` as the direction.

1. In event loop, we receive the incoming traffic as a READ event.
2. We read the traffic using `HTTPConnection::http_read` virtual method and the RPC is admitted into `RPCQueue` (if the connection is HTTP/2, we use a set of callbacks from `nghttp2` to read the traffic. See Backend services section to read more.)

### `ConnectionType::INGRESS` requests
3. Event loop calls `State::forward` to
3.1. Call `State::route_request` to find an `ConnectionDirection::UPSTREAM` connection.
3.2. Call `State::forward_request` to update `RPCMapper` and call `State::write_http` to write the traffic.

### `ConnectionType::EGRESS` requests
3. Following step 5 onwards for admitted requests of Ingress (We don't use `Ingress::Ingress`).

## Response
Similar to Ingress responses.


# Backend services
For these sidecars, we have gRPC at both sides. Same request flow as Frontend. The main difference is how `HTTP2Connection::http_read` reads the incoming traffic.

## `nghttp2` callback descriptions
1. event loop calls `HTTP2Connection::http_read`. This function uses `nghttp2` callbacks to parse the traffic.
1.1. `on_begin_headers_callback` is called when a new stream starts. We use this callback to allocate RPCMessage object.
1.2. `on_header_callback` is called for every individual parsed header. We write this into the allocated RPC.
1.3. `on_data_chunk_recv_callback` is called for reading the DATA frame (arguments).
1.4. `frame_recv_callback` is called for every recieved frame. However, we only use it to detect the boundariy of RPCs. When the end of RPC is detected, it adds it to the `RPCQueue`.
2. Similar procedure as Admitted requests of Frontend.