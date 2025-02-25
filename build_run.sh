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

# Run the sidecar
GLOG_logtostderr=1 ./sidecar