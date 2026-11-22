#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# --- glog ---
if [ ! -f "${SCRIPT_DIR}/glog_install/lib/libglog.a" ]; then
    if [ ! -d "${SCRIPT_DIR}/glog" ]; then
        echo "Downloading glog..."
        wget -q https://github.com/google/glog/archive/refs/tags/v0.7.1.tar.gz -O glog.tar.gz
        tar -xf glog.tar.gz
        mv glog-0.7.1 "${SCRIPT_DIR}/glog"
        rm glog.tar.gz
    fi

    echo "Building glog..."
    cd "${SCRIPT_DIR}/glog"
    # We need -fPIC because it will be linked into a position-independent executable (usually) or just good practice
    cmake -S . -B build \
        -DCMAKE_C_COMPILER=clang-18 \
        -DCMAKE_CXX_COMPILER=clang++-18 \
        -DCMAKE_CXX_FLAGS="-stdlib=libc++ -fPIC" \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DWITH_GFLAGS=OFF \
        -DWITH_UNWIND=OFF \
        -DBUILD_TESTING=OFF \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_PREFIX="${SCRIPT_DIR}/glog_install"
    cmake --build build -j$(nproc)
    cmake --install build
else
    echo "glog already built. Skipping."
fi

# --- googletest ---
if [ ! -f "${SCRIPT_DIR}/googletest_install/lib/libgtest.a" ]; then
    if [ ! -d "${SCRIPT_DIR}/googletest" ]; then
        echo "Downloading googletest..."
        wget -q https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz -O googletest.tar.gz
        tar -xf googletest.tar.gz
        mv googletest-1.14.0 "${SCRIPT_DIR}/googletest"
        rm googletest.tar.gz
    fi

    echo "Building googletest..."
    cd "${SCRIPT_DIR}/googletest"
    cmake -S . -B build \
        -DCMAKE_C_COMPILER=clang \
        -DCMAKE_CXX_COMPILER=clang++ \
        -DCMAKE_CXX_FLAGS="-stdlib=libc++ -fPIC" \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_GMOCK=OFF \
        -DINSTALL_GTEST=ON \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_PREFIX="${SCRIPT_DIR}/googletest_install"
    cmake --build build -j$(nproc)
    cmake --install build
else
    echo "googletest already built. Skipping."
fi

# --- liburing (static; noble's 2.5 lacks the futex helpers) ---
if [ ! -f "${SCRIPT_DIR}/liburing_install/lib/liburing.a" ]; then
    if [ ! -d "${SCRIPT_DIR}/liburing" ]; then
        echo "Downloading liburing..."
        wget -q https://github.com/axboe/liburing/archive/refs/tags/liburing-2.15.tar.gz -O liburing.tar.gz
        tar -xf liburing.tar.gz
        mv liburing-liburing-2.15 "${SCRIPT_DIR}/liburing"
        rm liburing.tar.gz
    fi

    echo "Building liburing..."
    cd "${SCRIPT_DIR}/liburing"
    ./configure --cc=clang-18 --prefix="${SCRIPT_DIR}/liburing_install"
    # -fPIC: the sidecar links as a PIE
    CFLAGS="-O3 -fPIC" make -C src -j$(nproc)
    make install
else
    echo "liburing already built. Skipping."
fi

# --- yaml-cpp ---
# We can reuse the existing script but ensure it's called or just call it here.
# The existing script handles its own checks.
echo "Building yaml-cpp..."
"${SCRIPT_DIR}/build-yaml-cpp.sh"

echo "All dependencies built."

