# Roshanfer sidecar

C++ HTTP sidecar: Agent, Ingress, and the Request Limit Protocol (RLP). Built with io_uring; YAML config on the CLI.

## Repository layout

```text
sidecar/
├── src/                       implementation (event loop, ingress, RLP, HTTP)
├── include/                   public headers for the same
├── docs/                      RPC flow, RLP, UDP internals, coding standards
├── external/                  NanoLog, HdrHistogram, yaml-cpp, glog
├── build.sh                   CMake wrapper (release / debug)
├── CMakeLists.txt             C++20 build
└── Dockerfile                 ubuntu:noble image
```
