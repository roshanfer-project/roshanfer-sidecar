#!/usr/bin/env python3
"""
Distributed Systems Log Viewer
A tool to visualize logs from multiple services in columns.
"""

import re
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from datetime import datetime
from dataclasses import dataclass

import typer
from rich.console import Console
from rich.text import Text
from rich.table import Table

app = typer.Typer(help="Visualize distributed system logs in columns")
DEFAULT_COLOR = "white"

# Hardcoded color mappings for logical units
# Edit these to customize colors for your logical units
COLOR_MAP = {
    "QM": "yellow",
    "PPMClient": "cyan",
    "RPCForward": "green",
    "INGRESS": "magenta",
    "config": DEFAULT_COLOR,
    "state": DEFAULT_COLOR,
    "main": DEFAULT_COLOR,
    "event_loop": DEFAULT_COLOR,
    "connection": DEFAULT_COLOR,
}


# Log line regex pattern
# Format: <SEVERITY> <TIMESTAMP> <FILE:LINE>] <Message>
LOG_PATTERN = re.compile(
    r'^([A-Z])\s+'  # Severity
    r'(\d{2}:\d{2}:\d{2}\.\d{6})\s+'  # Timestamp
    r'([^:]+:\d+)\]\s+'  # File:line
    r'(.*)$'  # Message
)


@dataclass
class LogEntry:
    """Represents a single log entry"""
    service: str
    severity: str
    timestamp: float  # Relative time in seconds
    file_line: str
    logical_unit: str
    message: str
    raw_timestamp: str


def parse_timestamp(ts_str: str) -> float:
    """Convert HH:MM:SS.microseconds to seconds since midnight"""
    h, m, s = ts_str.split(':')
    seconds = float(s) + int(m) * 60 + int(h) * 3600
    return seconds


def extract_logical_unit(message: str) -> Tuple[str, str]:
    """Extract logical unit from message (everything before first colon)"""
    if ':' in message:
        parts = message.split(':', 1)
        return parts[0].strip(), parts[1].strip()
    return "", message


def parse_metadata(message: str) -> Dict[str, str]:
    """Parse metadata fields from message (format: field: value| field: value| ...)"""
    metadata = {}
    # Split by | and extract field:value pairs
    parts = message.split('|')
    for part in parts:
        part = part.strip()
        if ':' in part:
            # Split on first colon only
            key, value = part.split(':', 1)
            metadata[key.strip()] = value.strip()
    return metadata


def extract_id_from_message(message: str) -> Optional[str]:
    """Extract request ID from log message metadata if present"""
    metadata = parse_metadata(message)
    return metadata.get('id', None)


def parse_log_file(file_path: Path, service_name: str) -> List[LogEntry]:
    """Parse a log file and return list of log entries"""
    entries = []
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.rstrip()
            match = LOG_PATTERN.match(line)
            
            if match:
                severity, timestamp_str, file_line, message = match.groups()
                logical_unit, clean_message = extract_logical_unit(message)
                
                entry = LogEntry(
                    service=service_name,
                    severity=severity,
                    timestamp=parse_timestamp(timestamp_str),
                    file_line=file_line,
                    logical_unit=logical_unit,
                    message=clean_message,
                    raw_timestamp=timestamp_str
                )
                entries.append(entry)
    
    return entries


def format_relative_time(seconds: float, base_time: float) -> str:
    """Format relative time as seconds.milliseconds.microseconds"""
    relative = seconds - base_time
    secs = int(relative)
    millis = int((relative * 1000) % 1000)
    micros = int((relative * 1000000) % 1000)
    return f"{secs:03d}.{millis:03d}.{micros:03d}"


def colorize_logical_unit(lu: str) -> Text:
    """Apply color to logical unit based on COLOR_MAP"""
    color = COLOR_MAP.get(lu, DEFAULT_COLOR)
    return Text(lu, style=color)




@app.command()
def main(
    log_files: List[Path] = typer.Argument(..., help="Log files to visualize"),
    filter_lu: Optional[List[str]] = typer.Option(
        None, "--filter", "-f", help="Filter by logical unit(s)"
    ),
    filter_id: Optional[List[str]] = typer.Option(
        None, "--filter-id", "--id", help="Filter by request ID(s)"
    ),
    max_width: int = typer.Option(
        45, "--width", "-w", help="Maximum column width"
    ),
    show_all: bool = typer.Option(
        False, "--all", "-a", help="Show all columns even if empty"
    ),
    force_color: bool = typer.Option(
        False, "--color", "-c", help="Force color output (useful when piping to file)"
    ),
    console_width: int = typer.Option(
        None, "--console-width", "-W", help="Console width"
    ),
):
    """
    Visualize distributed system logs in columns.
    
    Example:
        dslogs app.log backend.log frontend.log
        dslogs *.log --filter QM PPMClient
        dslogs app.log --filter-id 1 2 3  # Filter by request IDs
        dslogs app.log --color > output.log  # Preserve colors in file
    """
    
    # Create console with optional force_terminal for color preservation
    console = Console(force_terminal=force_color, width=console_width)
    
    # Validate files exist
    for f in log_files:
        if not f.exists():
            console.print(f"[red]Error: File not found: {f}[/red]")
            raise typer.Exit(1)
    
    # Parse all log files
    all_entries: List[LogEntry] = []
    services = []
    
    for log_file in log_files:
        service_name = log_file.stem  # filename without extension
        services.append(service_name)
        entries = parse_log_file(log_file, service_name)
        all_entries.extend(entries)
    
    if not all_entries:
        console.print("[yellow]No log entries found[/yellow]")
        return
    
    # Filter by logical unit if specified
    # Always show fatal (F) logs regardless of filter
    if filter_lu:
        filter_set = set(filter_lu)
        all_entries = [e for e in all_entries if e.severity == 'F' or e.logical_unit in filter_set]
        
        if not all_entries:
            console.print(f"[yellow]No entries found for logical units: {', '.join(filter_lu)}[/yellow]")
            return
    
    # Filter by request ID if specified
    # Always show fatal (F) logs regardless of filter
    if filter_id:
        id_set = set(filter_id)
        filtered_entries = []
        for e in all_entries:
            if e.severity == 'F':
                filtered_entries.append(e)
            else:
                entry_id = extract_id_from_message(e.message)
                if entry_id and entry_id in id_set:
                    filtered_entries.append(e)
        all_entries = filtered_entries
        
        if not all_entries:
            console.print(f"[yellow]No entries found for request IDs: {', '.join(filter_id)}[/yellow]")
            return
    
    # Sort by timestamp
    all_entries.sort(key=lambda e: e.timestamp)
    
    # Calculate base time (earliest log)
    base_time = all_entries[0].timestamp
    
    # Group entries by timestamp for display
    # We'll display one row per unique timestamp
    timestamp_groups: Dict[float, Dict[str, LogEntry]] = {}
    
    for entry in all_entries:
        if entry.timestamp not in timestamp_groups:
            timestamp_groups[entry.timestamp] = {}
        timestamp_groups[entry.timestamp][entry.service] = entry
    
    # Create table
    table = Table(show_header=True, header_style="bold", expand=False, padding=(0, 1))
    
    # Add time column
    table.add_column("Time", style="dim", width=15)
    
    # Add service columns
    for service in services:
        table.add_column(service, width=max_width, overflow="fold", no_wrap=False)
    
    # Add rows
    for timestamp in sorted(timestamp_groups.keys()):
        entries_at_time = timestamp_groups[timestamp]
        
        # Format relative time
        time_str = format_relative_time(timestamp, base_time)
        
        # Build row with Text object for time (consistent with other columns)
        time_cell = Text(time_str, style="dim")
        row = [time_cell]
        
        for service in services:
            if service in entries_at_time:
                entry = entries_at_time[service]
                
                # Build cell content with colored logical unit
                cell = Text()
                
                # Highlight fatal logs with red background
                if entry.severity == 'F':
                    cell.append("⚠️ FATAL ", style="bold red on black")
                
                # Add colored logical unit if present
                if entry.logical_unit:
                    cell.append_text(colorize_logical_unit(entry.logical_unit))
                    cell.append(": ")
                
                # Add full message (Rich will handle wrapping)
                msg_style = "bold red" if entry.severity == 'F' else ""
                cell.append(entry.message, style=msg_style)
                
                row.append(cell)
            else:
                row.append("")
        
        table.add_row(*row)
    
    # Print summary
    console.print(f"\n[bold]Viewing {len(log_files)} service(s): {', '.join(services)}[/bold]")
    console.print(f"Total log entries: {len(all_entries)}\n")
    
    # Print table
    console.print(table)


if __name__ == "__main__":
    app()

