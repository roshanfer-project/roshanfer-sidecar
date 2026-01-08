#!/home/farzad/files/venv/bin/python3

import argparse
import datetime
import math
import re
import sys
from collections import defaultdict
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

# Import plotting primitives (assuming it's in the same directory)
import plotting_primitives as pp

def parse_args():
    parser = argparse.ArgumentParser(description="Plot metric quantiles over time.")
    parser.add_argument("--files", nargs="+", help="List of log files to parse", required=True)
    parser.add_argument("--resolution", type=float, default=0.2, help="Time resolution in seconds (default: 0.2)")
    parser.add_argument("--output", type=str, default="metrics.pdf", help="Output PDF file (default: metrics.pdf)")
    return parser.parse_args()

def parse_logs(files):
    # Dictionary structure:
    # metrics[full_metric_name] = [(timestamp, value), ...]
    metrics = defaultdict(list)
    
    for filepath in files:
        try:
            with open(filepath, "r") as f:
                for line in f:
                    parts = line.strip().split(" ")
                    if len(parts) < 6:
                        continue
                    
                    # Log Format: Date Time ... M# Sidecar Metric ConnType Service:Method Value
                    # Example: 2025-12-29 18:56:56.192083527 ... M# app E2E INGRESS app2:GET 830
                    
                    # Try to find M# tag
                    try:
                        m_idx = parts.index("M#")
                    except ValueError:
                        continue
                    
                    if m_idx + 5 >= len(parts):
                        continue
                        
                    # Parse timestamp (parts[0] and parts[1])
                    date_str = parts[0]
                    time_str = parts[1]
                    
                    # Handle nanoseconds if present (truncate to microseconds for datetime)
                    if '.' in time_str:
                        hms, frac = time_str.split('.')
                        frac = frac[:6] # Truncate to 6 digits (microseconds)
                        time_str = f"{hms}.{frac}"
                    
                    dt_str = f"{date_str} {time_str}"
                    try:
                        dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
                        timestamp = dt.timestamp()
                    except ValueError:
                        # Fallback if parsing fails
                        continue

                    # Extract metric details
                    # sidecar_name = parts[m_idx + 1] # Not used in grouping for now, usually redundant if file per sidecar
                    metric_name = parts[m_idx + 2]
                    conn_type = parts[m_idx + 3]
                    rpc_path = parts[m_idx + 4]
                    try:
                        value = float(parts[m_idx + 5])
                    except ValueError:
                        continue

                    # define a unique name for the metric plot
                    full_metric_name = f"{metric_name} {conn_type} {rpc_path}"
                    metrics[full_metric_name].append((timestamp, value))
                    
        except Exception as e:
            print(f"Error reading file {filepath}: {e}", file=sys.stderr)
            
    return metrics

def calculate_quantiles(data, resolution, count_only=False, global_start=None, global_end=None):
    if not data and (global_start is None or global_end is None):
        return None, None, {}

    # Sort by timestamp (if data exists)
    if data:
        data.sort(key=lambda x: x[0])
        first_time = data[0][0]
        last_time = data[-1][0]
    else:
        first_time = global_start
        last_time = global_end
    
    start_time = global_start if global_start is not None else first_time
    end_time = global_end if global_end is not None else last_time
    
    # Create bins
    timestamps = []
    quantiles = {50: [], 95: [], 99: []}
    
    current_bin_start = start_time
    current_bin_values = []
    
    data_idx = 0
    n = len(data)
    
    # Skip data before start_time if any
    while data_idx < n and data[data_idx][0] < current_bin_start:
        data_idx += 1
    
    while current_bin_start <= end_time + resolution:
        bin_end = current_bin_start + resolution
        
        # Collect values in this bin
        while data_idx < n and data[data_idx][0] < bin_end:
            current_bin_values.append(data[data_idx][1])
            data_idx += 1
            
        # Calculate stats for the bin
        if current_bin_values:
            timestamps.append(current_bin_start - start_time) # Relative time
            if count_only:
                count_val = len(current_bin_values)
                quantiles[50].append(count_val)
                quantiles[95].append(count_val)
                quantiles[99].append(count_val)
            else:
                quantiles[50].append(np.percentile(current_bin_values, 50))
                quantiles[95].append(np.percentile(current_bin_values, 95))
                quantiles[99].append(np.percentile(current_bin_values, 99))
        else:
            timestamps.append(current_bin_start - start_time)
            # For count_only, empty bin means 0 count
            if count_only:
                quantiles[50].append(0)
                quantiles[95].append(0)
                quantiles[99].append(0)
            else:
                quantiles[50].append(np.nan)
                quantiles[95].append(np.nan)
                quantiles[99].append(np.nan)

        # Reset/Advance
        current_bin_values = []
        current_bin_start += resolution
        
    return timestamps, quantiles

def main():
    args = parse_args()
    
    print(f"Parsing logs: {args.files}")
    metrics_data = parse_logs(args.files)
    
    if not metrics_data:
        print("No metrics found.")
        return

    print(f"Found {len(metrics_data)} metrics. Generating plots...")
    
    # Determine global time range
    global_start = float('inf')
    global_end = float('-inf')
    
    has_data = False
    for metric_name, data in metrics_data.items():
        if data:
            data_times = [x[0] for x in data]
            global_start = min(global_start, min(data_times))
            global_end = max(global_end, max(data_times))
            has_data = True
            
    if not has_data:
        print("No valid data found.")
        return

    print(f"Global time range: {global_start} to {global_end} (Duration: {global_end - global_start:.2f}s)")
    
    with PdfPages(args.output) as pdf:
        style = pp.PlotStyle(width_points=160, hspace=0.5, wspace=0.3)
        
        sorted_keys = sorted(metrics_data.keys())
        
        # Grid settings
        rows = 3
        cols = 2
        plots_per_page = rows * cols
        
        # Process metrics in chunks
        for i in range(0, len(sorted_keys), plots_per_page):
            chunk = sorted_keys[i:i + plots_per_page]
            
            # Create a grid for this page
            grid = pp.SubplotGrid(style, layout=f"{rows}x{cols}")
            
            for j, metric_name in enumerate(chunk):
                is_count = "QS" in metric_name or "DROP" in metric_name or "DSC" in metric_name
                is_drop = "DROP" in metric_name
                
                row_idx = j // cols
                col_idx = j % cols
                ax = grid.get_ax(row_idx, col_idx)
                
                data = metrics_data[metric_name]
                # print(f"Processing metric: {metric_name}")
                timestamps, results = calculate_quantiles(data, args.resolution, count_only=is_drop,
                                                          global_start=global_start, global_end=global_end)
                
                
                if not timestamps:
                    continue
                
                if not is_count and not "Prob" in metric_name:
                    # scale to ms
                    results[50] = [x * 0.001 if not np.isnan(x) else x for x in results[50]]
                    results[95] = [x * 0.001 if not np.isnan(x) else x for x in results[95]]
                    results[99] = [x * 0.001 if not np.isnan(x) else x for x in results[99]]

                # Calculate max_val for ylim safely ignoring NaNs
                try:
                    # Filter out NaNs for max calculation
                    valid_values = [x for x in results[99] if not np.isnan(x)]
                    max_val = max(valid_values) if valid_values else 1.0 # Default to 1.0 if empty
                except ValueError:
                    max_val = 1.0
                
                if "E2E" in metric_name:
                    max_val = 16
                
                
                # Plot lines or scatter
                if "EMA" in metric_name or "HIST" in metric_name:
                    # p50
                    pp.plot_scatter(ax, timestamps, results[50], label="P50", style=style, color_idx=1)
                    # p95
                    pp.plot_scatter(ax, timestamps, results[95], label="P95", style=style, color_idx=4)
                    # p99
                    pp.plot_scatter(ax, timestamps, results[99], label="P99", style=style, color_idx=0)
                else:
                    # p50
                    pp.plot_line(ax, timestamps, results[50], label="P50", style=style, color_idx=1)
                    # p95
                    pp.plot_line(ax, timestamps, results[95], label="P95", style=style, color_idx=4)
                    # p99
                    pp.plot_line(ax, timestamps, results[99], label="P99", style=style, color_idx=0)
                
                # Shorten title if needed or just use metric name
                # metric_name structure: "Metric Conn Type Path"
                # Let's try to make it wrapped or smaller if too long, but standard title is fine for now
                grid.configure_ax(ax, 
                                  xlabel="Time (s)" if row_idx == rows - 1 else "", 
                                  ylabel="Count" if is_count else "Lat (ms)" if col_idx == 0 else "",
                                  title=metric_name,
                                  #log_y=False if str.rfind(metric_name, "QS") >= 0 else True
                                  ylim=(0, max_val*1.2),
                                  y_step=4 if "E2E" in metric_name else None
                                  )

            # Hide unused axes
            for k in range(len(chunk), plots_per_page):
                row_idx = k // cols
                col_idx = k % cols
                ax = grid.get_ax(row_idx, col_idx)
                ax.set_visible(False)

            # we only need one legend per page, shared
            grid.add_shared_legend(position="bottom", y_offset=0)
            
            # Save the page
            grid.fig.savefig(pdf, format='pdf', bbox_inches='tight')
            plt.close(grid.fig)
            
    print(f"Saved metrics to {args.output}")

if __name__ == "__main__":
    main()
