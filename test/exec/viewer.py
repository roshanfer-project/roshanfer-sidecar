#!/usr/bin/env python3
"""
metric_tree.py – Rich‑based tree view for a single configuration dump
--------------------------------------------------------------------
This tiny script prints your service‑level metrics as an **ASCII tree** (like
`tree(1)`) so you can eyeball one run at a time and diff runs manually.

Input format – unchanged
~~~~~~~~~~~~~~~~~~~~~~~~
```
{
  "search": {
    "latency_p99_ms": 120.5,
    "successful_qps": 1800
  },
  "frontend": {
    "latency_p99_ms": 80.2,
    "successful_qps": 1750
  }
}
```

Quick start
~~~~~~~~~~~
```bash
pip install rich              # once
python metric_tree.py run1.json
python metric_tree.py run1.json --metrics latency_p99_ms throughput
```

Flags
~~~~~
* `--metrics` *m1 m2 …* – show only these metrics.
* `--list-metrics` – show all metric names and exit.
* `--no-colour` – disable colouring if piping to file/CI.

"""

import argparse
import json
import os
import sys
from typing import Dict, List

from rich.console import Console
from rich.tree import Tree
from rich.theme import Theme

# ---------------------------------------------------------------------------
# Console setup (allow --no-colour)
# ---------------------------------------------------------------------------
def make_console(no_colour: bool) -> Console:
    return Console(theme=Theme({
        "service": "bold bright_blue",
        "metric": "white",
        "value": "cyan",
    }), color_system=None if no_colour else "auto")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: str) -> Dict[str, Dict[str, float]]:
    try:
        with open(path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        if not isinstance(data, dict):
            raise ValueError("top-level JSON must map services to metric dicts")
        return data
    except Exception as exc:
        console = make_console(False)
        console.print(f"[red]✖ Failed to load {path}: {exc}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="metric_tree",
        description="Show per-service metrics as an ASCII tree.")
    ap.add_argument("file", help="JSON metrics dump for one configuration point")
    ap.add_argument("--metrics", nargs="*", help="Only show these metrics")
    ap.add_argument("--list-metrics", action="store_true", help="List metrics present and exit")
    ap.add_argument("--no-colour", action="store_true", help="Disable colour output (plain text)")
    return ap.parse_args()


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def build_tree(
    data: Dict[str, Dict[str, float]], root_label: str, metric_filter: List[str]
) -> Tree:
    root = Tree(f"[bold]{root_label}[/bold]", guide_style="bright_black")

    def add_nodes(node: Tree, metrics: dict) -> bool:
        """Recursively add metrics; return True if any leaf was added."""
        added = False
        for key, val in sorted(metrics.items()):
            if isinstance(val, dict):
                # create a subgroup
                subgroup = node.add(f"[metric]{key}[/]")
                if add_nodes(subgroup, val):
                    added = True
                else:
                    # no leaves under this subgroup => remove it
                    node.children.remove(subgroup)
            else:
                # leaf value
                if not metric_filter or key in metric_filter:
                    # only float/int get .4g formatting
                    if isinstance(val, (int, float)):
                        display = f"{val:.4g}"
                    else:
                        display = str(val)
                    node.add(f"[metric]{key}[/]: [value]{display}[/]")
                    added = True
        return added

    for service, metrics in sorted(data.items()):
        svc_node = root.add(f"[service]{service}[/]")
        if not add_nodes(svc_node, metrics):
            # no matching leaves under this service => drop it
            root.children.remove(svc_node)

    return root

def build_config_summary():
    """Read yaml config files under the service-mesh/.configs directory and in each file, extract the ppm_limit.
    Note that the name of the service is the name of the file without the .yaml extension.
    """

    config_dir = os.path.join(os.path.dirname(__file__), "service-mesh", ".configs")
    config_files = [f for f in os.listdir(config_dir) if f.endswith(".yaml")]
    config_summary = {}
    for config_file in config_files:
        service_name = os.path.splitext(config_file)[0]
        with open(os.path.join(config_dir, config_file), "r") as f:
            for line in f:
                if "ppm_limit" in line:
                    ppm_limit = line.split(":")[1].strip()
                    config_summary[service_name] = ppm_limit
                    break
    return config_summary


def main() -> None:
    args = parse_args()
    console = make_console(args.no_colour)

    data = load_json(args.file)
    all_metrics = sorted({m for svc in data.values() for m in svc})

    if args.list_metrics:
        console.print("\n".join(all_metrics))
        return

    metric_filter = args.metrics or []
    if metric_filter:
        unknown = [m for m in metric_filter if m not in all_metrics]
        if unknown:
            console.print(f"[yellow]Warning:[/] unknown metrics requested – {', '.join(unknown)}")
    tree = build_tree(data, os.path.basename(args.file), metric_filter)

    

    """ config_summary = build_config_summary()
    config_tree = Tree("PPM Limits:", guide_style="bright_black")
    for service, ppm_limit in config_summary.items():
        config_tree.add(f"[service]{service}[/]: [value]{ppm_limit}[/]")
    
    console.print(config_tree) """
    console.print(tree)


if __name__ == "__main__":
    main()
