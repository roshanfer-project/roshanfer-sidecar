#!/home/farzad/files/venv/bin/python3

import argparse
import json
import os
import sys
import numpy as np
from collections import defaultdict
from rich.console import Console
from rich.tree import Tree
from rich.theme import Theme

def make_console(no_colour: bool) -> Console:
    return Console(theme=Theme({
        "span": "bold bright_blue",
        "stat": "white",
        "value": "cyan",
    }), color_system=None if no_colour else "auto")

def load_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        return data
    except Exception as exc:
        print(f"Failed to load {path}: {exc}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Analyze trace statistics from Chrome Tracing JSON.")
    parser.add_argument("file", help="JSON traces file")
    parser.add_argument("--no-colour", action="store_true", help="Disable colour output")
    args = parser.parse_args()

    console = make_console(args.no_colour)
    data = load_json(args.file)

    # Group spans by TID (Sidecar) -> Name
    # We only care about events with 'dur' (duration)
    spans_by_tid = defaultdict(lambda: defaultdict(list))
    
    for event in data:
        if 'dur' in event and 'name' in event and 'tid' in event:
            spans_by_tid[event['tid']][event['name']].append(event['dur'])

    if not spans_by_tid:
        console.print("[red]No spans found in the file.[/]")
        return

    root = Tree(f"[bold]Trace Statistics ({os.path.basename(args.file)})[/bold]", guide_style="bright_black")

    # Sort by TID
    for tid in sorted(spans_by_tid.keys()):
        tid_node = root.add(f"[bold magenta]{tid}[/]")
        
        spans_by_name = spans_by_tid[tid]
        # Sort by name for consistent output
        for name in sorted(spans_by_name.keys()):
            durations = spans_by_name[name]
            count = len(durations)
            mean = np.mean(durations)
            std_dev = np.std(durations)
            min_val = np.min(durations)
            max_val = np.max(durations)
            
            node = tid_node.add(f"[span]{name}[/]")
            node.add(f"[stat]Count:[/] [value]{count}[/]")
            node.add(f"[stat]Mean:[/] [value]{mean:.2f} us[/]")
            node.add(f"[stat]StdDev:[/] [value]{std_dev:.2f} us[/]")
            node.add(f"[stat]Min:[/] [value]{min_val:.2f} us[/]")
            node.add(f"[stat]Max:[/] [value]{max_val:.2f} us[/]")

    console.print(root)

if __name__ == "__main__":
    main()
