# Roshanfer sidecar

C++ HTTP sidecar: Agent, Ingress, and the PPM credit protocol. Built with io_uring; YAML config on the CLI.

## Repository layout

```text
sidecar/
├── src/                       implementation (event loop, ingress, PPM, HTTP)
├── include/                   public headers for the same
├── docs/                      RPC flow, PPM, UDP internals, coding standards
├── external/                  NanoLog, HdrHistogram, yaml-cpp, glog
├── build.sh                   CMake wrapper (release / debug)
├── CMakeLists.txt             C++20 build
└── Dockerfile                 ubuntu:noble image
```
