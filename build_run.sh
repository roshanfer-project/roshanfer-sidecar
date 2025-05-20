#!/bin/bash

# Build the sidecar
cd ./build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi

# check if config file is provided
if [ -z "$1" ]; then
    echo "Config file not provided"
    exit 1
fi

# check if there is a second argument
if [ -z "$2" ]; then
    GLOG_logtostderr=1 GLOG_v=2 ./sidecar $1
    exit 0
fi

GLOG_logtostderr=1 GLOG_v=2 ./sidecar $1 2> $2