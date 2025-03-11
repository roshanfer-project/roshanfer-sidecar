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

# Run the sidecar and pipe stderr to a log file
# Check if -l flag is provided
if [[ "$1" != "-l" ]]; then
    GLOG_logtostderr=1 ./sidecar
    exit 0
fi
GLOG_logtostderr=1 ./sidecar 2> sidecar.log