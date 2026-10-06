from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger("transform")

# Tokens that source systems use to mean "no value".
NULL_TOKENS = {"", "n/a", "na", "null", "none", "nan", "-", "?"}

CATEGORY_MAP = {
    "electronics": "Electronics",
    "sports": "Sports",
    "clothing": "Clothing",
    "home & kitchen": "Home & Kitchen",
    "home and kitchen": "Home & Kitchen",
    "books": "Books",
    "beauty": "Beauty",
    "toys": "Toys",
}

COUNTRY_MAP = {
    "united states": "United States", "usa": "United States", "u.s.": "United States",
    "us": "United States", "u.s.a.": "United States",
    "united kingdom": "United Kingdom", "uk": "United Kingdom", "u.k.": "United Kingdom",
    "england": "United Kingdom", "great britain": "United Kingdom",
    "sri lanka": "Sri Lanka", "lk": "Sri Lanka", "srilanka": "Sri Lanka",
    "india": "India", "in": "India",
    "australia": "Australia", "au": "Australia",
    "germany": "Germany", "de": "Germany", "deutschland": "Germany",
    "canada": "Canada", "ca": "Canada",
    "singapore": "Singapore", "sg": "Singapore",
    "united arab emirates": "United Arab Emirates", "uae": "United Arab Emirates",
    "u.a.e.": "United Arab Emirates",
    "japan": "Japan", "jp": "Japan",
}

PAYMENT_MAP = {
    "credit card": "Credit Card", "credit_card": "Credit Card", "cc": "Credit Card",
    "debit card": "Debit Card", "debit": "Debit Card",
    "paypal": "PayPal", "pay pal": "PayPal",
    "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery", "cash": "Cash on Delivery",
    "bank transfer": "Bank Transfer", "wire": "Bank Transfer",
}

# Date formats emitted by the source system, tried in order. Slash dates are
# day-first (dd/mm/yyyy) by agreement with the source, so we never guess.
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%b %d %Y", "%Y/%m/%d %H:%M", "%d-%b-%Y"]

UNKNOWN = "Unknown"
UNCATEGORIZED = "Uncategorized"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _clean_text(s: pd.Series) -> pd.Series:
    """Trim, collapse internal whitespace, and turn null tokens into NaN."""
    s = s.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    return s.mask(s.str.lower().isin(NULL_TOKENS))


def _map_aliases(s: pd.Series, mapping: dict[str, str]) -> tuple[pd.Series, pd.Series]:
    """Map case-insensitive aliases to canonical values.

    Returns (mapped, unmapped_mask): unmapped_mask is True where a non-null
    value had no known alias.
    """
    mapped = s.str.lower().map(mapping)
    unmapped = s.notna() & mapped.isna()
    return mapped.astype("string"), unmapped


def _canonical_spelling(s: pd.Series) -> pd.Series:
    """Collapse case variants ("USB-C HUB", "usb-c hub") onto the most frequent spelling.

    Better than .str.title(), which would mangle names like "USB-C Hub" -> "Usb-C Hub".
    """
    key = s.str.casefold()
    counts = pd.DataFrame({"key": key, "val": s}).dropna().value_counts()
    # value_counts is sorted desc, so the first spelling per key is the most frequent
    best = counts.reset_index().drop_duplicates("key").set_index("key")["val"]
    return key.map(best).astype("string")


def _parse_money(s: pd.Series) -> pd.Series:
    stripped = s.str.replace(r"(?i)usd|\$|,|\s", "", regex=True)
    return pd.to_numeric(stripped, errors="coerce").round(2)


def _parse_dates(s: pd.Series) -> pd.Series:
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in DATE_FORMATS:
        todo = out.isna() & s.notna()
        if not todo.any():
            break
        out[todo] = pd.to_datetime(s[todo], format=fmt, errors="coerce")
    return out.dt.normalize()


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------
def standardize(raw: pd.DataFrame) -> pd.DataFrame:
    """Return a typed, standardized copy of the raw frame.

    Adds `_issues`-style helper columns (prefixed with `_`) that the validator
    uses to explain why a value could not be standardized.
    """
    df = pd.DataFrame({"source_row": raw["source_row"]})

    # --- identifiers & free text -------------------------------------------------
    df["order_id"] = _clean_text(raw["order_id"]).str.upper()
    df["customer_name"] = _clean_text(raw["customer_name"]).str.title()
    df["customer_email"] = _clean_text(raw["customer_email"]).str.lower()
    df["product_name"] = _canonical_spelling(_clean_text(raw["product_name"]))

    # --- category: alias map, then impute from the product's usual category ------
    category_raw = _clean_text(raw["category"])
    category, unknown_cat = _map_aliases(category_raw, CATEGORY_MAP)
    category = category.mask(unknown_cat, category_raw.str.title())
    missing_cat = category.isna()
    product_to_cat = (
        pd.DataFrame({"p": df["product_name"], "c": category})
        .dropna()
        .groupby("p")["c"]
        .agg(lambda x: x.mode().iat[0])
    )
    category = category.fillna(df["product_name"].map(product_to_cat))
    imputed = int((missing_cat & category.notna()).sum())
    still_missing = int(category.isna().sum())
    df["category"] = category.fillna(UNCATEGORIZED)
    log.info(
        "category: imputed %s from product history, %s set to '%s'",
        imputed, still_missing, UNCATEGORIZED,
    )

    # --- numerics -------------------------------------------------------------------
    quantity_raw = _clean_text(raw["quantity"])
    quantity = pd.to_numeric(quantity_raw, errors="coerce")
    df["_quantity_raw"] = quantity_raw
    # non-integer quantities (e.g. "1.5") are invalid, not something to round
    df["quantity"] = quantity.where(quantity.isna() | (quantity == np.floor(quantity))).astype("Int64")

    price_raw = _clean_text(raw["unit_price"])
    df["_unit_price_raw"] = price_raw
    df["unit_price"] = _parse_money(price_raw)

    rating_raw = _clean_text(raw["rating"])
    df["_rating_raw"] = rating_raw
    df["rating"] = pd.to_numeric(rating_raw, errors="coerce").round(1)

    # --- country: alias map; missing -> 'Unknown' ---------------------------------
    country_raw = _clean_text(raw["country"])
    country, unknown_country = _map_aliases(country_raw, COUNTRY_MAP)
    if unknown_country.any():
        log.warning(
            "country: %s values with no known alias kept as title case: %s",
            int(unknown_country.sum()), sorted(country_raw[unknown_country].unique())[:10],
        )
    country = country.mask(unknown_country, country_raw.str.title())
    log.info("country: %s missing values set to '%s'", int(country.isna().sum()), UNKNOWN)
    df["country"] = country.fillna(UNKNOWN)

    # --- dates ----------------------------------------------------------------------
    date_raw = _clean_text(raw["order_date"])
    df["_order_date_raw"] = date_raw
    df["order_date"] = _parse_dates(date_raw)

    # --- payment method: alias map; missing -> 'Unknown' -------------------------
    pay_raw = _clean_text(raw["payment_method"])
    payment, unknown_pay = _map_aliases(pay_raw, PAYMENT_MAP)
    df["_payment_raw"] = pay_raw
    df["_payment_unknown"] = unknown_pay
    df["payment_method"] = payment.mask(pay_raw.isna(), UNKNOWN)

    log.info("Standardized %s rows", f"{len(df):,}")
    return df


def deduplicate(
    raw: pd.DataFrame, df: pd.DataFrame, valid: pd.Series | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Remove duplicates in two passes.

    1. exact duplicates: identical raw rows (e.g. the same batch exported twice)
    2. business-key duplicates: same normalized order_id (e.g. " ord-000123").
       The first *valid* occurrence in the file wins (`valid` marks rows that
       pass validation), so a broken first copy can't knock out a good later
       one. If no copy is valid, the first one is kept and gets rejected by
       validation with its real reasons.

    Returns (deduplicated_df, duplicates_df). duplicates_df has a `reject_reason`.
    """
    raw_cols = [c for c in raw.columns if c != "source_row"]
    exact_mask = raw.duplicated(subset=raw_cols, keep="first").to_numpy()
    is_valid = np.ones(len(df), dtype=bool) if valid is None else valid.to_numpy(dtype=bool)

    # order the remaining rows valid-first, then by file position; the first per order_id wins
    idx = np.flatnonzero(~exact_mask)
    idx = idx[np.lexsort((idx, ~is_valid[idx]))]
    ids = df["order_id"].iloc[idx]
    key_dup = ids.notna().to_numpy() & ids.duplicated(keep="first").to_numpy()
    key_mask = np.zeros(len(df), dtype=bool)
    key_mask[idx[key_dup]] = True

    dupes = pd.concat(
        [
            df.loc[exact_mask, ["source_row"]].assign(reject_reason="duplicate: exact duplicate row"),
            df.loc[key_mask, ["source_row"]].assign(reject_reason="duplicate: order_id already seen"),
        ]
    )

    log.info(
        "Deduplication: %s exact duplicates, %s duplicate order_ids removed",
        int(exact_mask.sum()), int(key_mask.sum()),
    )
    return df[~(exact_mask | key_mask)].copy(), dupes
