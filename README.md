# Roshanfer Sidecar

Linux C++ sidecar for the Roshanfer research mesh. It sits next to a microservice, proxies HTTP/1 and HTTP/2 (gRPC), and uses a UDP credit protocol (PPM) so a service only receives work up to its current limit.

This repository builds a single binary, `sidecar`. There is no `cmake --install` target and no in-tree sample `config.yaml`.

## Overview

The process is a multi-threaded `io_uring` event loop (`src/main.cc`, `src/event_loop.cc`). You pass one YAML file. That file selects the role and the listeners, upstream pools, and service graph for this hop.

Typical topology, from `docs/ppm_logic.md`:

```
External clients -- HTTP/1 (no PPM) --> Ingress -- HTTP/1 + PPM --> Frontend -- HTTP/2 + PPM --> Backend --> ...
```

Roles are flags in the same binary:

| Role | Config | Data path (from `src/event_loop.cc` / `src/state.cc`) |
| --- | --- | --- |
| Ingress | `is_ingress: true` | HTTP/1 on the mesh-facing (EGRESS) listener. Pending requests sit in `Ingress`’s deque. PPM DNs go to the frontend; overflow is head-dropped with HTTP 503. |
| Frontend | `is_frontend: true` | HTTP/1 toward the local service (`INGRESS` listener / pool). HTTP/2 toward the mesh (`EGRESS`). Mesh RPCs wait in `PPMQueue` until a credit arrives. |
| Backend / other mesh | both flags false | HTTP/2 on both sides. Same PPM client/server path as frontend EGRESS. |

`is_plain_frontend` only relaxes checks that inbound RPCs already carry `rpc-local-id` and `priority`.

## Project structure

```
.
├── src/                  C and C++ sources (compiled into one executable)
├── include/              Headers used by those sources
├── docs/                 Protocol and implementation notes (not a user guide)
├── external/             Build-time dependencies (git submodules + download scripts)
├── CMakeLists.txt        C++20 target `sidecar`; pulls in deps via external/setup_deps.sh
├── build.sh              CMake wrapper pinned to Clang 18
├── Dockerfile            Copies a prebuilt build/sidecar; does not compile
├── docker-compose.yaml   Builds that image only
├── crash_bt.gdb          gdb batch script (copied into the image; gdb entrypoint is commented out)
├── LICENSE               MIT
└── THIRD_PARTY.md        Licenses for some external/ components
```

Important source files:

| Path | Role |
| --- | --- |
| `src/main.cc` | CLI: `sidecar <config_file>`; starts one `EventLoop` thread per `num_threads` |
| `src/config.cc` | YAML schema and validation |
| `src/event_loop.cc` | `io_uring` accept/read/write/connect/UDP completions |
| `src/state.cc` | Routing, connection pools, PPM send/recv, fan-out |
| `src/ingress.cc` | Ingress admission queue, AIMD cap, 503 drops, `rpc-id` / `priority` injection |
| `src/connection.cc` | HTTP/1 (`picohttpparser`) and HTTP/2 (`nghttp2`) |
| `src/credit_queue.cc` | Server-side DN wait queue, global + per-endpoint limits, 3 priority weights |
| `src/ppm_queue.cc` | Client-side RPCs waiting for a credit (non-ingress mesh path) |
| `include/stats.h` | Latency / occupancy estimators used by AIMD and limit updates |

`docs/` (read these for protocol details):

- `docs/ppm_logic.md` — PPM message types, active-request accounting, roles
- `docs/rpc_flow.md` — ingress / frontend / backend HTTP paths
- `docs/udp_ppm_internals.md` — UDP socket, io_uring buffer groups, dispatch
- `docs/rtt.md` — PPM header timing fields
- `docs/rpc_ids.md` — `rpc-id` / `rpc-local-id` packing
- `docs/adding_headers.md` — HTTP/1 vs HTTP/2 header-length rules
- `docs/faq.md` — one known mapper/timeout case
- `docs/coding_standards.md` — two in-tree coding rules

`external/`:

- `NanoLog`, `HdrHistogram_c`, `yaml-cpp` — git submodules required to configure CMake
- `setup_deps.sh` — builds those plus Google glog v0.7.1 (downloaded on first configure)
- `.gitmodules` also lists `test/rwg`; that directory is not present in this tree

## Installation

### Prerequisites

From `CMakeLists.txt`, `build.sh`, and `external/setup_deps.sh`:

- Linux with `io_uring`
- CMake ≥ 3.15
- Clang 18 toolchain: `clang-18`, `clang++-18`, `llvm-ar-18`, `llvm-ranlib-18`
- libc++ 18 headers at `/usr/lib/llvm-18/include/c++/v1` (CMake fatals otherwise). On Debian/Ubuntu: `libc++-18-dev` and `libc++abi-18-dev` (needed if a newer `libc++-*-dev` is also installed)
- `pkg-config` modules `liburing` and `libnghttp2`
- `wget` and `make` (glog / NanoLog / yaml-cpp builds)
- Git submodules under `external/` initialized

On Ubuntu, the compile-time packages that match those checks are typically:

```bash
sudo apt-get install -y \
  cmake clang-18 libc++-18-dev libc++abi-18-dev \
  pkg-config liburing-dev libnghttp2-dev wget make
```

Initialize the submodules this CMake build actually uses:

```bash
git submodule update --init external/NanoLog external/HdrHistogram_c external/yaml-cpp
```

### Build

```bash
./build.sh release
```

`build.sh` requires a CMake build type (`release`, `RelWithDebInfo`, `Debug`, …), configures with Clang 18, and writes the binary to `build/sidecar`. Parallelism is `JOBS` or `nproc`.

Optional NanoLog metric compile (`CMakeLists.txt` option `SIDECAR_ENABLE_NANOLOG`; logs to `/compressedLog` in `src/main.cc`):

```bash
SIDECAR_ENABLE_NANOLOG=1 ./build.sh release
```

There is no install prefix. Use `build/sidecar` directly, or copy it (the `Dockerfile` expects `build/sidecar` at image build time).

First CMake configure runs `external/setup_deps.sh` and fails if that script fails.

## Usage

### Binary

```text
./build/sidecar <config_file>
```

Anything other than exactly one argument prints that usage line and exits 1.

Logging is glog (`google::InitGoogleLogging`). Use glog’s usual flags or `GLOG_*` environment variables (for example `GLOG_logtostderr=1`) if you want logs on stderr instead of glog’s default log directory.

Each worker thread is named `<config.name>-s-<index>` (`pthread_setname_np` in `src/main.cc`).

### Configuration

`src/config.cc` loads YAML. Keys below are the ones the parser reads.

**Always required**

| Key | Meaning |
| --- | --- |
| `ring_size` | `io_uring` ring size |
| `buffer_count` | TCP + UDP buffer-pool count (`BufferManager`) |
| `buffer_size` | Buffer bytes; must be ≤ `HTTP1Connection_BUF_SIZE` (200000) |
| `num_threads` | Worker threads. Ingress: must equal the number of `mapping` entries. Non-ingress: must be 1 (RPC id map / stats are per-thread; see the fatal in `config.cc`) |
| `egress_listener_port` | EGRESS TCP listen port for non-ingress roles |
| `ingress_listener_port` | INGRESS TCP listen port. Non-ingress also binds PPM UDP here (`SO_REUSEPORT`) |
| `ingress_upstream_host` / `ingress_upstream_port` | Local service for the INGRESS connection pool (frontend / backend) |
| `name` | Process / thread-name prefix and NanoLog metric tag |

**Role and pools**

| Key | Default / rule |
| --- | --- |
| `is_ingress` | `false`. Requires `ingress_pool_connections`. Forbids `pfanout` / `dfanout`. Each mapping must be 1:1 (downstream name equals the mapping key). Ingress EGRESS port comes from that mapping’s `listen_port` |
| `is_frontend` | `false`. Requires `frontend_pool_connections` |
| `is_plain_frontend` | `false` |
| `over_commitment` | Required if not ingress; float in `[0, 1]`. Used in leaf PPM global-limit formula |
| `cpu_count` | Required if not ingress; integer ≥ 1. Seeds per-endpoint limits |
| `extra_limit` | `0`. Added to computed limits |
| `ingress_pool_connections` | HTTP/1 pool size to each `routing` upstream when `is_ingress` |
| `frontend_pool_connections` | HTTP/1 pool size to `ingress_upstream_*` when `is_frontend` |

**`routing` (optional map)** — keys are downstream service names. Each value needs `upstream.host` and `upstream.port`. Optional `slo` (milliseconds; ingress AIMD uses `slo * 1000` microseconds) and `priority`. Ingress requires both `slo` and `priority` for the hosted service; ingress rejects `priority` outside `0..3`.

**`mapping` (optional map)** — keys are hosted (upstream) service names. Fields: `downstreams` (list; max 255; `dfanout` max 10), optional `listen_port`, `pfanout`, `dfanout`.

Format example (every key is parsed in `src/config.cc`; sizes, ports, and hosts are not defaults from this repo — fill them for your deployment):

```yaml
ring_size: 4096
buffer_count: 4096
buffer_size: 16384
num_threads: 1
egress_listener_port: 9000
ingress_listener_port: 9001
ingress_upstream_host: "127.0.0.1"
ingress_upstream_port: 8080
name: "frontend"

is_ingress: false
is_frontend: true
is_plain_frontend: false
over_commitment: 0.0
cpu_count: 1
extra_limit: 0
frontend_pool_connections: 4

routing:
  backend-a:
    upstream:
      host: "10.0.0.2"
      port: 9000
    slo: 20
    priority: 1

mapping:
  frontend:
    downstreams: ["backend-a"]
    pfanout: false
    dfanout: false
```

Ingress additionally needs `is_ingress: true`, `ingress_pool_connections`, `mapping.*.listen_port`, a 1:1 `downstreams` list, and `slo` / `priority` on the matching `routing` entry. Mesh nodes that are neither ingress nor frontend omit those two flags and use HTTP/2 on both listeners.

Services that emit mesh requests must send `rpc-id` and, on the EGRESS path, `rpc-local-id` (and usually `priority`) as described in `docs/rpc_ids.md`.

### Docker

`Dockerfile` does **not** compile. It copies `build/sidecar` into an `ubuntu:noble` image with `liburing` and a libc++ runtime.

```bash
./build.sh release
docker build -t sidecar-sidecar:latest .
```

`docker-compose.yaml` only builds that image (`sidecar-sidecar:latest` from the service name).

The image entrypoint is `/sidecar`; the default argument is `/config.yaml` (not included in the image). Supply a file at that path, or pass another path as the container command. Optional `PROC_NAME` sets the process title (`exec -a`).

```bash
docker run --rm \
  -e PROC_NAME=frontend-sidecar \
  -v /absolute/path/to/config.yaml:/config.yaml \
  sidecar-sidecar:latest
```

`crash_bt.gdb` is in the image; the gdb `ENTRYPOINT` in the Dockerfile is commented out.

## Important features

These exist in the current sources:

- **io_uring I/O** — accept, TCP read/write/connect, UDP `recvmsg`/`sendmsg`, provided-buffer rings (`bgid` 0 TCP, `bgid` 1 PPM). See `src/ring_wrapper.cc`.
- **PPM credit protocol** — demand notification (DN), credit grant, credit return (`0x02`). No retries or timeouts (`docs/ppm_logic.md`). Ingress keeps at most one DN in flight and does not use `PPMQueue` for external HTTP.
- **Admission and limits** — ingress AIMD on `ingress_size_cap` from smoothed p99 end-to-end latency vs `routing.slo`; mesh servers grant from `CreditQueue` under a global PPM limit and per-endpoint limits (updated from RTT / local service time in `State::update_limits`). Leaf services (`routing` empty) get a finite global limit; intermediate services set it to `INT_MAX`.
- **Priority credit scheduling** — three inner queues, weighted 16 / 4 / 1 (`src/credit_queue.cc`).
- **Fan-out** — sequential (default), parallel (`pfanout`), and dynamic (`dfanout`: DN all downstreams, forward the chosen branch, return unused credits).
- **HTTP/1 and HTTP/2** — picohttpparser and nghttp2; protocol chosen from role + listener type in `EventLoop::run`.
- **Connection pools** — pre-`CONNECT` to configured upstreams; HTTP/1 uses many connections, HTTP/2 uses one multiplexed connection per route (or `frontend_pool_connections` HTTP/1 sockets to the local frontend).
- **RPC identity** — `rpc-id` (global) and packed `rpc-local-id` for hop-local maps (`docs/rpc_ids.md`).
- **Stats** — EMA / time-weighted means / t-digest / HdrHistogram; optional NanoLog `M#` lines when built with `SIDECAR_ENABLE_NANOLOG`.
- **Hardening / warnings** — libc++ extensive hardening, `-Wall -Wextra` and several `-Werror=*` flags in CMake.

## License

MIT. See `LICENSE`. Third-party licenses: `THIRD_PARTY.md` (NanoLog, HdrHistogram_c, yaml-cpp). glog is fetched by `external/setup_deps.sh`. `src/picohttpparser.c` and `src/tdigest.c` carry their own upstream headers.
