"""
Streaming I/O helpers for reading URL lists and writing NDJSON results.
"""

import sys
from typing import Any, Dict, Iterable

import orjson


def read_urls(path: str) -> Iterable[str]:
    """Yield URLs from a file; supports newline- or comma-separated entries."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                # Handle comma-separated URLs on the same line
                for url in line.split(","):
                    url = url.strip()
                    if url:  # Skip empty URLs after splitting
                        yield url


def write_ndjson_line(d: Dict[str, Any]) -> None:
    """Write dictionary as one NDJSON line to stdout using orjson."""
    sys.stdout.write(orjson.dumps(d).decode() + "\n")
