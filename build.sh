#!/bin/bash

# Ensure a build type is provided as an argument
if [ $# -lt 1 ]; then
    echo "Usage: $0 <build_type>"
    exit 1
fi

BUILD_TYPE=$1
echo "Build type set to: $BUILD_TYPE"

# Build the sidecar
cd ./build
cmake ..   -DCMAKE_BUILD_TYPE=$BUILD_TYPE
cmake --build .

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
else 
    echo "Build successful"
fi

