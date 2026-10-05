from __future__ import annotations

import logging
from datetime import date

import pandas as pd

log = logging.getLogger("validate")

EMAIL_PATTERN = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
ORDER_ID_PATTERN = r"^ORD-\d{6}$"
MIN_ORDER_DATE = pd.Timestamp("2000-01-01")
MAX_QUANTITY = 1000

OUTPUT_COLUMNS = [
    "order_id", "customer_name", "customer_email", "product_name", "category",
    "quantity", "unit_price", "rating", "country", "order_date", "payment_method",
]


def build_rules(df: pd.DataFrame, today: date) -> list[tuple[str, pd.Series]]:
    """Each rule is (reason, mask_of_failing_rows)."""
    today_ts = pd.Timestamp(today)
    q, p, r, d = df["quantity"], df["unit_price"], df["rating"], df["order_date"]

    return [
        ("order_id missing", df["order_id"].isna()),
        ("order_id bad format", df["order_id"].notna() & ~df["order_id"].str.match(ORDER_ID_PATTERN, na=False)),
        ("product_name missing", df["product_name"].isna()),
        ("customer_name missing", df["customer_name"].isna()),
        ("customer_email invalid",
         df["customer_email"].notna() & ~df["customer_email"].str.match(EMAIL_PATTERN, na=False)),
        ("quantity missing", df["_quantity_raw"].isna()),
        ("quantity not an integer", df["_quantity_raw"].notna() & q.isna()),
        ("quantity <= 0", q.notna() & (q <= 0)),
        (f"quantity > {MAX_QUANTITY}", q.notna() & (q > MAX_QUANTITY)),
        ("unit_price missing", df["_unit_price_raw"].isna()),
        ("unit_price not numeric", df["_unit_price_raw"].notna() & p.isna()),
        ("unit_price <= 0", p.notna() & (p <= 0)),
        ("rating not numeric", df["_rating_raw"].notna() & r.isna()),
        ("rating out of range 1-5", r.notna() & ((r < 1) | (r > 5))),
        ("order_date missing", df["_order_date_raw"].isna()),
        ("order_date unparseable", df["_order_date_raw"].notna() & d.isna()),
        ("order_date in the future", d.notna() & (d > today_ts)),
        ("order_date before 2000", d.notna() & (d < MIN_ORDER_DATE)),
        ("payment_method unknown value", df["_payment_unknown"]),
    ]


def validate(df: pd.DataFrame, today: date | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (valid_rows, rejected_rows). Rejected rows carry every failed rule."""
    today = today or date.today()
    reasons = pd.Series("", index=df.index, dtype="string")

    for reason, mask in build_rules(df, today):
        mask = mask.fillna(False).astype(bool)
        n = int(mask.sum())
        if n:
            log.info("  rule failed: %-30s %5s rows", reason, n)
            reasons = reasons.mask(mask, reasons.where(reasons == "", reasons + "; ") + reason)

    bad = reasons != ""
    rejected = df.loc[bad, ["source_row"]].assign(reject_reason=reasons[bad].astype(str))
    valid = df.loc[~bad, ["source_row", *OUTPUT_COLUMNS]].copy()
    valid["order_date"] = valid["order_date"].dt.date

    log.info("Validation: %s valid, %s rejected", f"{len(valid):,}", f"{len(rejected):,}")
    return valid, rejected
