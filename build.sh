#!/bin/bash

# Ensure a build type is provided as an argument
if [ $# -lt 1 ]; then
    echo "Usage: $0 <build_type>"
    exit 1
fi

BUILD_TYPE=$1
echo "Build type set to: $BUILD_TYPE"

# Build the sidecar
# Create build directory if it doesn't exist
if [ ! -d "./build" ]; then
    mkdir ./build
    echo "Created build directory"
fi
cd ./build
EXTRA_CMAKE=()
if [ "${SIDECAR_ENABLE_NANOLOG:-}" = "1" ]; then
  EXTRA_CMAKE+=(-DSIDECAR_ENABLE_NANOLOG=ON)
  echo "SIDECAR_ENABLE_NANOLOG=1 -> NanoLog metrics enabled in binary"
else
  EXTRA_CMAKE+=(-DSIDECAR_ENABLE_NANOLOG=OFF)
fi
cmake .. -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_AR=/usr/bin/llvm-ar-18 -DCMAKE_RANLIB=/usr/bin/llvm-ranlib-18 "${EXTRA_CMAKE[@]}"
cmake --build .

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
else 
    echo "Build successful"
fi

