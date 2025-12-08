#!/bin/bash

docker container logs ingress-sidecar &> ingress.log

docker container logs app-sidecar &> app.log

#docker container logs backend1-sidecar &> backend1.log

#docker container logs backend2-sidecar &> backend2.log

# Build dslogs command with optional ID filter
DSLOGS_ARGS=(
    "./dslogs.py"
    "./ingress.log" "./app.log"
    "--filter" "QM"
    "--filter" "PPMClient"
    "--filter" "RPCForward"
    "--filter" "INGRESS"
    "--color"
    "-w" "30"
    "-W" "210"
)

# Add ID filter if provided
if [ $# -gt 0 ]; then
    DSLOGS_ARGS+=("--filter-id")
    for id in "$@"; do
        DSLOGS_ARGS+=("$id")
    done
fi

# Execute the command and redirect to trace.log
"${DSLOGS_ARGS[@]}" > trace.log