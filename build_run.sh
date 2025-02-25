#!/bin/bash

# Build the sidecar
cd ./build
cmake .. -DCMAKE_BUILD_TYPE=Debug
cmake --build .

# Run the sidecar
GLOG_logtostderr=1 ./sidecar