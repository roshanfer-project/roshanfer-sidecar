#!/usr/bin/env python3
"""
Find request IDs that were admitted but never got a response.

Scans a log file for:
- INGRESS: New request to be admitted (with id and service metadata)
- RPCForward: EGRESS response (with id and service metadata)

Reports request IDs that have admission but no corresponding response.
"""

import re
import sys
import argparse
from pathlib import Path
from typing import Dict, Set, Tuple

# Log line regex pattern
# Format: <SEVERITY> <TIMESTAMP> <FILE:LINE>] <Message>
LOG_PATTERN = re.compile(
    r'^([A-Z])\s+'  # Severity
    r'(\d{2}:\d{2}:\d{2}\.\d{6})\s+'  # Timestamp
    r'([^:]+:\d+)\]\s+'  # File:line
    r'(.*)$'  # Message
)


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


def parse_log_file(file_path: Path) -> Tuple[Dict[Tuple[str, str], str], Set[Tuple[str, str]]]:
    """
    Parse log file and extract:
    - admitted_requests: dict mapping (service, id) -> timestamp
    - responded_requests: set of (service, id) tuples
    
    Returns: (admitted_requests, responded_requests)
    """
    admitted_requests = {}  # (service, id) -> timestamp
    responded_requests = set()  # set of (service, id) tuples
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.rstrip()
            match = LOG_PATTERN.match(line)
            
            if not match:
                continue
            
            severity, timestamp_str, file_line, message = match.groups()
            
            # Check for admission log
            if 'INGRESS: New request to be admitted' in message:
                metadata = parse_metadata(message)
                service = metadata.get('service', '')
                req_id = metadata.get('id', '')
                if service and req_id:
                    admitted_requests[(service, req_id)] = timestamp_str
            
            # Check for response log
            elif 'RPCForward: EGRESS response' in message:
                metadata = parse_metadata(message)
                service = metadata.get('service', '')
                req_id = metadata.get('id', '')
                if service and req_id:
                    responded_requests.add((service, req_id))
    
    return admitted_requests, responded_requests


def main():
    """Find request IDs that were admitted but never got a response."""
    parser = argparse.ArgumentParser(
        description="Find request IDs that were admitted but never got a response",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example: find_missing_responses.py ingress.log"
    )
    parser.add_argument("log_file", type=Path, help="Log file to analyze")
    
    args = parser.parse_args()
    
    if not args.log_file.exists():
        print(f"Error: File not found: {args.log_file}", file=sys.stderr)
        sys.exit(1)
    
    admitted_requests, responded_requests = parse_log_file(args.log_file)
    
    # Find missing responses
    missing_responses = {}
    for (service, req_id), timestamp in admitted_requests.items():
        if (service, req_id) not in responded_requests:
            if service not in missing_responses:
                missing_responses[service] = []
            missing_responses[service].append((req_id, timestamp))
    
    # Output results
    if not missing_responses:
        print(f"No missing responses found in {args.log_file}")
        print(f"Total admitted requests: {len(admitted_requests)}")
        print(f"Total responses: {len(responded_requests)}")
        return
    
    print(f"Found {sum(len(ids) for ids in missing_responses.values())} request(s) without responses:\n")
    
    # Group by service for cleaner output
    for service in sorted(missing_responses.keys()):
        print(f"Service: {service}")
        for req_id, timestamp in sorted(missing_responses[service], key=lambda x: int(x[0]) if x[0].isdigit() else 0):
            print(f"  id: {req_id} (admitted at {timestamp})")
        print()
    
    print(f"Summary:")
    print(f"  Total admitted requests: {len(admitted_requests)}")
    print(f"  Total responses: {len(responded_requests)}")
    print(f"  Missing responses: {sum(len(ids) for ids in missing_responses.values())}")


if __name__ == "__main__":
    main()

