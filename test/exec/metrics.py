#!/home/farzad/files/venv/bin/python3

import numpy as np
import argparse
from datetime import datetime
import json
import os

def main():
    parser = argparse.ArgumentParser(prog="metrics")
    parser.add_argument("--file", type=str, required=True)
    parser.add_argument("--window", type=float, default=0,
                        help="Window size in seconds for rate over time plot (e.g., 0.1 for 100ms)")
    parser.add_argument("--no-print", action="store_true", help="Disable printing of additional output")
    args = parser.parse_args()

    metrics = {}
    utilization = {}

    with open(args.file, "r") as file:
        lines = file.readlines()
        for line in lines:
            try:
                parts = line.strip().split(" ")
                if len(parts) < 5:
                    continue
                
                # Find the index of "U#" tag for utilization lines
                u_tag_idx = None
                for i, part in enumerate(parts):
                    if part == "U#":
                        u_tag_idx = i
                        break
                
                if u_tag_idx is not None and u_tag_idx + 4 < len(parts):
                    # Format: ... U# <sidecar_name> UTILIZATION <service> <utilization_value>
                    tag = parts[u_tag_idx]
                    sidecar_name = parts[u_tag_idx + 1]
                    util_keyword = parts[u_tag_idx + 2]
                    service = parts[u_tag_idx + 3]
                    utilization_value = parts[u_tag_idx + 4]
                    timestamp = parts[0]  # First part is timestamp
                    
                    if util_keyword == "UTILIZATION":
                        if sidecar_name not in utilization:
                            utilization[sidecar_name] = {}
                        if service not in utilization[sidecar_name]:
                            utilization[sidecar_name][service] = {"values": [], "timestamps": []}
                        
                        utilization[sidecar_name][service]["values"].append(float(utilization_value))
                        utilization[sidecar_name][service]["timestamps"].append(timestamp)
                    continue
                
                # Find the index of "M#" tag for metrics lines
                m_tag_idx = None
                for i, part in enumerate(parts):
                    if part == "M#":
                        m_tag_idx = i
                        break
                
                if m_tag_idx is not None and m_tag_idx + 5 < len(parts):
                    # Format: ... M# <sidecar_name> <metric_name> <connection_type> <service>:<method> <value>
                    tag = parts[m_tag_idx]
                    service_name = parts[m_tag_idx + 1]
                    metric_name = parts[m_tag_idx + 2]
                    conn_type = parts[m_tag_idx + 3]
                    rpc_path = parts[m_tag_idx + 4]
                    value = parts[m_tag_idx + 5]
                    timestamp = parts[0]  # First part is timestamp
                else:
                    continue
                    
            except (ValueError, IndexError):
                continue

            if tag == "M#":
                if service_name not in metrics:
                    metrics[service_name] = {}
                if metric_name not in metrics[service_name]:
                    metrics[service_name][metric_name] = {}
                if conn_type not in metrics[service_name][metric_name]:
                    metrics[service_name][metric_name][conn_type] = {}
                if rpc_path not in metrics[service_name][metric_name][conn_type]:
                    metrics[service_name][metric_name][conn_type][rpc_path] = {"values": [], "timestamps": []}

                metrics[service_name][metric_name][conn_type][rpc_path]["values"].append(int(value))
                #metrics[service_name][metric_name][conn_type][rpc_path]["timestamps"].append(timestamp)
    
    # Process metrics
    export = {}
    for service_name, data in metrics.items():
        export[service_name] = {}
        print(rf"/////////////////////////////////////////////  {service_name}  \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\")
        for metric_name, content in data.items():
            export[service_name][metric_name] = {}
            for conn_type, rpc_data in content.items():
                export[service_name][metric_name][conn_type] = {}
                for rpc_path, rpc_content in rpc_data.items():
                    name = f"{metric_name}  {conn_type}  {rpc_path}"
                    if not args.no_print:
                        print(f"###########   {name}   ###########")
                    res = percentiles(rpc_content["values"], [50, 95, 99], no_print=args.no_print)
                    export[service_name][metric_name][conn_type][rpc_path] = res
                    #print_info(rpc_content["values"], rpc_content["timestamps"], rate_window=args.window)

        if not os.path.exists("metrics.json"):
            with open("metrics.json", "w") as tmp_file:
                tmp_file.write("{}")

        with open("metrics.json", "r+") as json_file:
            content = json_file.read()
            if content:
                json_dict = json.loads(content)
            else:
                json_dict = {}
            json_dict[service_name] = export[service_name]
            json_file.seek(0)
            json.dump(json_dict, json_file)
            json_file.truncate()

    # Process utilization data
    if utilization:
        utilization_export = {}
        print(rf"\n\n==============================================  UTILIZATION  ==============================================")
        for sidecar_name, services in utilization.items():
            utilization_export[sidecar_name] = {}
            print(rf"-------------------------------------------  {sidecar_name}  -------------------------------------------")
            for service, service_data in services.items():
                if not args.no_print:
                    print(f"Service: {service}")
                    print(f"  Count: {len(service_data['values'])}")
                    print(f"  Mean: {np.mean(service_data['values']):.4f}")
                    print(f"  Min: {np.min(service_data['values']):.4f}")
                    print(f"  Max: {np.max(service_data['values']):.4f}")
                    print(f"  Std: {np.std(service_data['values']):.4f}")
                
                utilization_export[sidecar_name][service] = {
                    "values": service_data["values"],
                    "timestamps": service_data["timestamps"],
                    "count": len(service_data["values"]),
                    "mean": float(np.mean(service_data["values"])),
                    "min": float(np.min(service_data["values"])),
                    "max": float(np.max(service_data["values"])),
                    "std": float(np.std(service_data["values"]))
                }

        # Export utilization data to utilization.json (merge with existing data)
        existing_utilization = {}
        if os.path.exists("utilization.json"):
            try:
                with open("utilization.json", "r") as util_file:
                    existing_utilization = json.load(util_file)
            except (json.JSONDecodeError, FileNotFoundError):
                existing_utilization = {}
        
        # Merge the new utilization data with existing data
        for sidecar_name, services in utilization_export.items():
            if sidecar_name not in existing_utilization:
                existing_utilization[sidecar_name] = {}
            for service_name, service_data in services.items():
                if service_name not in existing_utilization[sidecar_name]:
                    existing_utilization[sidecar_name][service_name] = {
                        "values": [],
                        "timestamps": [],
                        "count": 0,
                        "mean": 0.0,
                        "min": float('inf'),
                        "max": float('-inf'),
                        "std": 0.0
                    }
                
                # Merge values and timestamps
                existing_utilization[sidecar_name][service_name]["values"].extend(service_data["values"])
                existing_utilization[sidecar_name][service_name]["timestamps"].extend(service_data["timestamps"])
                
                # Recalculate statistics for the merged data
                all_values = existing_utilization[sidecar_name][service_name]["values"]
                existing_utilization[sidecar_name][service_name]["count"] = len(all_values)
                existing_utilization[sidecar_name][service_name]["mean"] = float(np.mean(all_values))
                existing_utilization[sidecar_name][service_name]["min"] = float(np.min(all_values))
                existing_utilization[sidecar_name][service_name]["max"] = float(np.max(all_values))
                existing_utilization[sidecar_name][service_name]["std"] = float(np.std(all_values))
        
        # Write the merged data back to utilization.json
        with open("utilization.json", "w") as util_file:
            json.dump(existing_utilization, util_file, indent=2)

def print_info(data, timestamps, bins=10, bar_char='█', width=40, rate_window=0):
    # Print latency histogram as text output
    hist, bin_edges = np.histogram(data, bins=bins)
    max_count = max(hist)
    print("\nLatency Histogram (us):")
    for i in range(len(hist)):
        count = hist[i]
        bar_len = int((count / max_count) * width)
        bar = bar_char * bar_len
        bin_range = f"{int(bin_edges[i])}-{int(bin_edges[i+1])}".rjust(10)
        print(f"{bin_range}: {bar} ({count})")

def percentiles(data, percentiles, no_print=False):
    """Calculate the specified percentiles of the data."""
    result = {}
    if not no_print:
        print(f"Count: {len(data)}")
    for p in percentiles:
        result[p] = f"{np.percentile(data, p):.2f}"
        if not no_print:
            print(f"{p}th: {result[p]}")
    result["count"] = f"{len(data)}"
    return result

if __name__ == "__main__":
    main()