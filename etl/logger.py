"""Logging setup: human-readable console output + a per-run log file."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from etl.config import LOG_DIR

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-14s | %(message)s"


def setup_logging(run_id: str, level: str = "INFO") -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"pipeline_{run_id}.log"

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_FORMAT, datefmt="%H:%M:%S"))
    root.addHandler(console)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(_FORMAT))
    root.addHandler(file_handler)

    # keep third-party libraries quiet
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return log_file
