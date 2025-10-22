#!/bin/bash

# Multi-API wrapper script for hotel benchmark
# Usage: $0 PROTOCOL BASE RATE DURATION "API1,API2,..." OUTPUT_DIR

# Check if required arguments are provided
if [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ] || [ -z "$5" ] || [ -z "$6" ]; then
  echo "Error: Missing required arguments"
  echo "Usage: $0 PROTOCOL BASE RATE DURATION \"API1,API2,...\" OUTPUT_DIR"
  exit 1
fi

protocol=$1
BASE=$2
RATE=$3
DURATION=$4
APIS=$5
output_dir=$6

# Split APIs by comma
IFS=',' read -ra API_ARRAY <<< "$APIS"

# Array to store background process PIDs
pids=()

# Function to run RWG for a single API
run_single_api() {
    local api=$1
    local output_file="$output_dir/out-$api.csv"
    
    if [ "$protocol" == "grpc" ]; then
        echo "Only HTTP is allowed"
        return 1
    else
        if [ "$api" = "app" ]; then
            url="http://localhost:3000/app"
        elif [ "$api" = "app2" ]; then
            url="http://localhost:3009/app2"
        else
            echo "Unknown API: $api"
            return 1
        fi
        ./rwg/rwg run --url $url -d exp -D 5,$DURATION -r $BASE,$RATE -w 200 -o $output_file --args service=$api
    fi
}

# If only one API, run directly
if [ ${#API_ARRAY[@]} -eq 1 ]; then
    run_single_api "${API_ARRAY[0]}"
    exit $?
fi

# For multiple APIs, run them in parallel
echo "Running ${#API_ARRAY[@]} APIs in parallel: ${APIS}"

for api in "${API_ARRAY[@]}"; do
    echo "Starting RWG for API: $api"
    run_single_api "$api" &
    pids+=($!)
done

# Wait for all background processes to complete
exit_code=0
for pid in "${pids[@]}"; do
    wait $pid
    if [ $? -ne 0 ]; then
        echo "Process $pid failed"
        exit_code=1
    fi
done

echo "All APIs completed with exit code: $exit_code"
exit $exit_code
