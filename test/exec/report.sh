#!/bin/bash

if [ -d ".log" ]; then
    rm -rf ".log"
fi

mkdir ".log"
cd ".log"

names=("app" "ingress" "backend1")

for name in "${names[@]}"; do
    c_name="${name}-sidecar"
    #docker container logs $c_name &> "${c_name}.log"
    docker container cp $c_name:./compressedLog "${c_name}.clog"
    ../../../external/NanoLog/runtime/decompressor decompress "${c_name}.clog" > "${c_name}.log"
    ../metrics.py --file "${c_name}.log" --no-print
done

# Run distributed tracing
../trace.py --files ingress-sidecar.log app-sidecar.log backend1-sidecar.log --percentiles 99 --count 10
mv traces.json ../traces.json
echo "Generated traces.json in $(pwd)/../traces.json"