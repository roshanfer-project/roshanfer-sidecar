# Request Flow

# Ingress
We are effectively using `Egress` path because it fits better with `ppm` logic. Only HTTP/1 is used here.

## Request

1. In event loop, we receive the incoming traffic as a READ event.
2. We read the traffic in the corresponding connection (HTTP/1) using `http_read` virtual methods provided by connections (Executing the `Downstream` routine).
2.1. The connection allocates the RPCMessage options (using RPCMapper).
2.2. The connection enqueues the RPCMessage in the `Ingress`.
3. Event loop asks the `State` to check admitting from ingress by calling `ingress_admit`.

### Admitted request

4. Upon admission, ingress adds the RPC to `RPCQueue`.
5. Later, event loop calls `ppm_client` that reads from `RPCQueue` and
5.1 Routes them (finding an apppropriate upstream connection for them) using `route_request`.
5.2 Adds the to `PPMQueue`.
5.3 Sends a DN request for it.
6. When we receive a DN response, the event loop executes the `RCVMSG` operation by calling `ppm_client` again but this time setting the first argyment to `true`.
7. If a valid credit has been recieved, `ppm_client` pops from `PPMQueue` and calls`forward_request` that maps the downstream and upstream connections and stream ids can calls the `write_http` from `State`.
8. `write_http` uses the `http_write` virtual method from connections to write the rpc to the `iouring`.

### Dropped request
4. In `ingress.check_drop` we prepare the request for dropping (e.g., setting the status code), map it to upstream connection (drop connection with `drop_fd` fd).
5. Add the RPC to `RPCQueue` of the upstream side.
6. Then, in `ingress_admit` we call `state.forward` that calls `state.write_http`.
5. `write_http` uses virtual `http_write` methods to write back the response.

## Response

1. In event loop, we receive the incoming traffic as a READ event.
2. We read the traffic in the corresponding connection (HTTP/1) using `http_read` virtual methods provided by connections (Executing `UPSTREAM` routine).
3. `http_read` reads the response into the RPC and adds it to `RPCQueue`.
4. Later, event loop calls `state.forward` that calls `state.write_http`.
5. `write_http` uses virtual `http_write` methods to write back the response.


# Frontend
For this sidecar, we have HTTP/1 for `Ingress` but gRPC for `Egress`.

## Request
Similar to admitted Ingress requests.

## Response
Similar to Ingress responses.


# Backend services
For these sidecars, we have gRPC at both sides

## Request
1. event loop calls `HTTP2Connection::http_read`. This function uses `nghttp2` callbacks to parse the traffic.
1.1. `on_begin_headers_callback` is called when a new stream starts. We use this callback to allocate RPCMessage object.
1.2. `on_header_callback` is called for every individual parsed header. We write this into the allocated RPC.
1.3. `on_data_chunk_recv_callback` is called for reading the DATA frame (arguments).
1.4. `frame_recv_callback` is called for every recieved frame. However, we only use it to detect the boundariy of RPCs. When the end of RPC is detected, it adds it to the `RPCQueue`.
2. Similar procedure as Admitted requests of Ingress.


## Response
Similar to Ingress responses.