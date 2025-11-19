#!/bin/bash

# Build script for yaml-cpp
# This script builds yaml-cpp once and creates a static library

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YAML_CPP_DIR="${SCRIPT_DIR}/yaml-cpp"
BUILD_DIR="${YAML_CPP_DIR}/build"

# Check if already built
if [ -f "${BUILD_DIR}/libyaml-cpp.a" ]; then
    echo "yaml-cpp already built. Skipping build."
    exit 0
fi

echo "Building yaml-cpp..."

# Create build directory
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Configure with CMake
cmake .. \
    -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DCMAKE_BUILD_TYPE=Release \
    -DYAML_CPP_BUILD_TESTS=OFF \
    -DYAML_CPP_BUILD_TOOLS=OFF \
    -DYAML_CPP_BUILD_CONTRIB=OFF \
    -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_CXX_FLAGS="-stdlib=libc++ -w" \
    -DCMAKE_EXE_LINKER_FLAGS="-stdlib=libc++"

# Build
make -j$(nproc)

echo "yaml-cpp built successfully!"
echo "Library: ${BUILD_DIR}/libyaml-cpp.a"
