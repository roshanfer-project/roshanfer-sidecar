#!/bin/bash

# Build the sidecar
cd ./build
cmake .. -DCMAKE_BUILD_TYPE=Debug
cmake --build .

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi

# run in gdb for debugging and pass environment variable to log to stderr
GLOG_logtostderr=1
gdb ./sidecar