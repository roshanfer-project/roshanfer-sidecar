#!/bin/bash

sudo docker compose down -v

names=("app" "backend1")

for name in "${names[@]}"; do
    name_full="${name}.o"
    if pgrep -f "$name_full" > /dev/null; then
        echo "Process '$name_full' exists. killing..."
        pkill -f -9 "$name_full"
    else
        echo "Process '$name_full' not running."
    fi
    sidecar_name="${name}-sidecar"
    envoy_name="${name}-envoy"
    docker container stop -t 0 $sidecar_name
    docker container rm -f $sidecar_name
    docker container stop -t 0 $envoy_name
    docker container rm -f $envoy_name
done

#if pgrep "sidecar" > /dev/null; then
#    echo "Killing all 'sidecar' processes..."
#    pkill -9 "sidecar"
#else
#    echo "No 'sidecar' processes found."
#fi

cd service-mesh-$1
docker compose down -v --remove-orphans
docker compose -f envoy-compose.yaml down -v --remove-orphans
docker container prune -f
#docker image prune -f