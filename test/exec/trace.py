#!/home/farzad/files/venv/bin/python3

import argparse
import json
import os
import re
from collections import defaultdict
import numpy as np

def main():
    parser = argparse.ArgumentParser(description="Process sidecar logs for distributed tracing.")
    parser.add_argument("--files", nargs='+', required=True, help="List of log files to process")
    parser.add_argument("--percentiles", type=str, default="50,99", help="Comma-separated list of percentiles to sample (e.g., 50,99)")
    parser.add_argument("--count", type=int, default=1, help="Number of traces to sample per percentile")
    parser.add_argument("--output", type=str, default="traces.json", help="Output JSON file path")
    args = parser.parse_args()

    percentiles = [float(p) for p in args.percentiles.split(",")]
    
    # Data structures
    # traces[rpc_id] = [ {timestamp, sidecar, metric, type, service, method, value}, ... ]
    traces = defaultdict(list)

    # Regex for T# lines
    # Format: Timestamp ... T# <rpc-id> <sidecar> <metric> <type> <service>:<method> <value>
    # Example: 2023-10-27 10:00:00.123456 ... T# 123 sidecar1 E2E HTTP1 service:method 1000
    # Note: The user changed rpc-id to int in the code, so we expect an integer.
    # We need to be flexible with the timestamp part as it might vary.
    # We'll look for "T#" and parse subsequent fields.
    
    print(f"Processing {len(args.files)} files...")

    for file_path in args.files:
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} not found.")
            continue
            
        with open(file_path, 'r') as f:
            for line in f:
                if "T#" not in line:
                    continue
                
                try:
                    parts = line.strip().split()
                    t_index = -1
                    for i, part in enumerate(parts):
                        if part == "T#":
                            t_index = i
                            break
                    
                    if t_index == -1 or t_index + 6 >= len(parts):
                        continue

                    # Real log format:
                    # 2025-12-01 14:12:56.509935815 /path/to/file:line NOTICE[0]: T# 1 app E2E INGRESS app:GET 5629
                    
                    # Extract timestamp (first two parts)
                    timestamp_str = parts[0] + " " + parts[1]
                    
                    # Parse fields after T#
                    # T# is at t_index
                    # t_index+1: rpc_id
                    # t_index+2: sidecar
                    # t_index+3: metric
                    # t_index+4: conn_type
                    # t_index+5: service:method
                    # t_index+6: value
                    
                    rpc_id = int(parts[t_index + 1])
                    sidecar = parts[t_index + 2]
                    metric = parts[t_index + 3]
                    conn_type = parts[t_index + 4]
                    service_method = parts[t_index + 5]
                    value = int(parts[t_index + 6])
                    
                    # Parse service and method
                    if ':' in service_method:
                        service, method = service_method.split(':', 1)
                    else:
                        service = service_method
                        method = ""

                    traces[rpc_id].append({
                        'timestamp_str': timestamp_str,
                        'sidecar': sidecar,
                        'metric': metric,
                        'type': conn_type,
                        'service': service,
                        'method': method,
                        'value': value,
                        'full_line': line
                    })
                    
                except (ValueError, IndexError) as e:
                    # print(f"Error parsing line: {line.strip()} - {e}")
                    continue

    print(f"Found {len(traces)} traces.")

    # Calculate trace latencies
    # We define trace latency as the E2E duration reported by the 'ingress' sidecar.
    # If not found, use the max E2E duration found in the trace.
    trace_latencies = []
    
    for rpc_id, spans in traces.items():
        root_span = None
        max_duration = 0
        
        for span in spans:
            if span['metric'] == 'E2E':
                if 'ingress' in span['sidecar']: # Heuristic: sidecar name contains 'ingress'
                    root_span = span
                    break
                if span['value'] > max_duration:
                    max_duration = span['value']
        
        latency = root_span['value'] if root_span else max_duration
        trace_latencies.append((latency, rpc_id))

    if not trace_latencies:
        print("No traces found.")
        return

    # Sort by latency
    trace_latencies.sort(key=lambda x: x[0])
    latencies = [x[0] for x in trace_latencies]
    
    # Calculate percentiles
    p_values = np.percentile(latencies, percentiles)
    
    selected_rpc_ids = set()
    
    print("Sampling traces:")
    for p, p_val in zip(percentiles, p_values):
        # Find traces closest to this value
        # We can use binary search or just linear scan since we are sorted.
        # For simplicity, let's find the index closest.
        
        # Find insertion point
        idx = np.searchsorted(latencies, p_val)
        
        # Get candidates around idx
        candidates = []
        start = max(0, idx - args.count)
        end = min(len(latencies), idx + args.count + 1)
        
        for i in range(start, end):
            candidates.append((abs(latencies[i] - p_val), trace_latencies[i][1], latencies[i]))
        
        # Sort by distance to p_val
        candidates.sort(key=lambda x: x[0])
        
        print(f"  P{p} (target: {p_val:.2f} us):")
        for i in range(min(args.count, len(candidates))):
            diff, rpc_id, lat = candidates[i]
            if rpc_id not in selected_rpc_ids:
                selected_rpc_ids.add(rpc_id)
                print(f"    - Trace {rpc_id}: {lat} us (diff: {diff:.2f})")

    # Generate Chrome Tracing JSON
    trace_events = []
    
    # Helper to parse timestamp to micros
    def parse_ts(ts_str):
        # 2025-12-01 12:26:47.123456
        try:
            # We can ignore the date part if all logs are same day, but better be safe.
            # Actually, let's just use string manipulation to get seconds and micros.
            # This is faster than datetime.strptime
            date_part, time_part = ts_str.split(' ')
            h, m, s = time_part.split(':')
            seconds = float(s)
            total_seconds = int(h) * 3600 + int(m) * 60 + seconds
            return total_seconds * 1_000_000
        except:
            return 0

    for rpc_id in selected_rpc_ids:
        spans = traces[rpc_id]
        
        # Group by Sidecar -> Connection Type
        # Structure: Sidecar -> Connection -> [Metrics]
        
        sidecar_groups = defaultdict(lambda: defaultdict(list))
        
        for span in spans:
            sidecar_groups[span['sidecar']][span['type']].append(span)
            
        # Create events
        # We need to calculate start times.
        # Log timestamp is the END time.
        # Start = End - Duration.
        
        pid = f"Trace {rpc_id}"
        
        for sidecar, conn_groups in sidecar_groups.items():
            tid = sidecar
            
            # We need a parent span for the Sidecar.
            # Its range is min(start) to max(end) of all its children.
            sidecar_start = float('inf')
            sidecar_end = float('-inf')
            
            conn_events = []
            
            for conn_type, metrics in conn_groups.items():
                # Parent span for Connection Type
                conn_start = float('inf')
                conn_end = float('-inf')
                
                metric_events = []
                
                for span in metrics:
                    end_ts = parse_ts(span['timestamp_str'])
                    duration = span['value']
                    start_ts = end_ts - duration
                    
                    conn_start = min(conn_start, start_ts)
                    conn_end = max(conn_end, end_ts)
                    
                    # Metric Span
                    # Name: <Metric> <Service>:<Method>
                    name = f"{span['metric']} {span['service']}:{span['method']}"
                    
                    metric_events.append({
                        "name": name,
                        "cat": "metric",
                        "ph": "X",
                        "ts": start_ts,
                        "dur": duration,
                        "pid": pid,
                        "tid": tid,
                        "args": {
                            "service": span['service'],
                            "method": span['method'],
                            "metric": span['metric'],
                            "value": span['value']
                        }
                    })
                
                # Create Connection Span
                if conn_start != float('inf'):
                    sidecar_start = min(sidecar_start, conn_start)
                    sidecar_end = max(sidecar_end, conn_end)
                    
                    conn_events.append({
                        "name": conn_type,
                        "cat": "connection",
                        "ph": "X",
                        "ts": conn_start,
                        "dur": conn_end - conn_start,
                        "pid": pid,
                        "tid": tid
                    })
                    
                # Add metric events
                trace_events.extend(metric_events)
            
            # Add connection events
            trace_events.extend(conn_events)
            
            # Create Sidecar Span
            if sidecar_start != float('inf'):
                trace_events.append({
                    "name": sidecar,
                    "cat": "sidecar",
                    "ph": "X",
                    "ts": sidecar_start,
                    "dur": sidecar_end - sidecar_start,
                    "pid": pid,
                    "tid": tid
                })

    with open(args.output, 'w') as f:
        json.dump(trace_events, f, indent=2)
        
    print(f"Exported {len(trace_events)} events to {args.output}")

if __name__ == "__main__":
    main()
