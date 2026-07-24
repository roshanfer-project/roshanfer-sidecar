#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

BUILD_TYPE="${1:-release}"
JOBS="${JOBS:-$(nproc)}"

echo "Build type: ${BUILD_TYPE}"
./build.sh "${BUILD_TYPE}"

ctest --test-dir build --output-on-failure -j"${JOBS}"
