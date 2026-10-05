"""Extract step: read the raw CSV exactly as delivered."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger("extract")

EXPECTED_COLUMNS = [
    "order_id",
    "customer_name",
    "customer_email",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "rating",
    "country",
    "order_date",
    "payment_method",
]


def extract(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Raw input not found: {path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")

    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Raw file is missing required columns: {sorted(missing)}")

    df = df[EXPECTED_COLUMNS].copy()
    # 1-based line number in the source file (excluding header) for traceability
    df.insert(0, "source_row", range(1, len(df) + 1))

    log.info("Extracted %s rows x %s columns from %s", f"{len(df):,}", len(EXPECTED_COLUMNS), path.name)
    return df
