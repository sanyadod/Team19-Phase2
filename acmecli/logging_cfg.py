"""
Logging setup controlled by LOG_LEVEL and LOG_FILE.
Silent (0), Info (1), Debug (2). Ensures log directory exists.
"""

import logging
import os
from pathlib import Path


def setup_logging() -> None:
    """Configure logging per environment; create parent dir if needed."""
    # Environment-driven logging levels for operational flexibility
    level_map = {
        "0": logging.CRITICAL + 1,  # Silent mode for production automation
        "1": logging.INFO,  # Standard operational logging
        "2": logging.DEBUG,  # Detailed debugging and tracing
    }

    lvl = level_map.get(os.getenv("LOG_LEVEL", "0"), logging.CRITICAL + 1)
    path = os.getenv("LOG_FILE", "acmecli.log")

    # Ensure the directory for the log file exists (create if missing)
    try:
        log_path = Path(path)
        # If LOG_FILE points to an existing directory, reject it and fall back
        if log_path.exists() and log_path.is_dir():
            raise ValueError("LOG_FILE points to a directory, not a file")
        log_dir = log_path.parent if str(log_path.parent) != "" else Path(".")
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        # If path is invalid or directory creation fails, fall back to default file in CWD
        path = "acmecli.log"

    # Production-ready logging format for monitoring and analysis
    logging.basicConfig(
        filename=path,
        level=lvl,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
