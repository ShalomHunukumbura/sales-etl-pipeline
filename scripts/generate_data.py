"""Generate a synthetic, deliberately *dirty* sales-transactions dataset.

The goal is to simulate a real-world raw export (e.g. from a POS / e-commerce
system) with the kinds of problems a data engineer actually has to deal with:

* exact duplicate rows and duplicate order_ids with formatting differences
* missing values (rating, country, category, email, price, product)
* mixed date formats and some unparseable / future dates
* inconsistent casing, stray whitespace, country aliases
* prices stored as strings with currency symbols and thousands separators
* out-of-range values (negative quantity, rating 7, invalid emails)

Usage:
    python scripts/generate_data.py                       # 12,000 rows -> data/raw/sales_raw.csv
    python scripts/generate_data.py --rows 50000 --seed 7
"""

from __future__ import annotations

import argparse
import csv
import random
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "raw" / "sales_raw.csv"

# product name -> (canonical category, base unit price)
PRODUCTS: dict[str, tuple[str, float]] = {
    "Wireless Mouse": ("Electronics", 24.99),
    "Mechanical Keyboard": ("Electronics", 89.00),
    "27in Monitor": ("Electronics", 229.00),
    "Noise Cancelling Headphones": ("Electronics", 199.99),
    "USB-C Hub": ("Electronics", 39.50),
    "Laptop Pro 14": ("Electronics", 1299.00),
    "Smartphone X": ("Electronics", 899.00),
    "Running Shoes": ("Sports", 79.95),
    "Yoga Mat": ("Sports", 25.00),
    "Dumbbell Set": ("Sports", 120.00),
    "Tennis Racket": ("Sports", 149.00),
    "Cotton T-Shirt": ("Clothing", 15.99),
    "Denim Jeans": ("Clothing", 49.99),
    "Winter Jacket": ("Clothing", 139.00),
    "Leather Belt": ("Clothing", 29.00),
    "Coffee Maker": ("Home & Kitchen", 59.99),
    "Blender": ("Home & Kitchen", 45.00),
    "Cookware Set": ("Home & Kitchen", 175.00),
    "Desk Lamp": ("Home & Kitchen", 32.50),
    "Data Engineering Handbook": ("Books", 42.00),
    "Python Crash Course": ("Books", 35.99),
    "Mystery Novel": ("Books", 12.99),
    "Face Moisturizer": ("Beauty", 22.50),
    "Perfume 50ml": ("Beauty", 68.00),
    "Hair Dryer": ("Beauty", 54.99),
    "Building Blocks Set": ("Toys", 64.99),
    "Board Game": ("Toys", 34.99),
    "Remote Control Car": ("Toys", 49.00),
}

# canonical country -> aliases a messy source system might emit
COUNTRIES: dict[str, list[str]] = {
    "United States": ["United States", "USA", "U.S.", "us", "united states"],
    "United Kingdom": ["United Kingdom", "UK", "U.K.", "england", "Great Britain"],
    "Sri Lanka": ["Sri Lanka", "sri lanka", "SRI LANKA", "LK", "Srilanka"],
    "India": ["India", "india", "IN", "INDIA"],
    "Australia": ["Australia", "AU", "australia"],
    "Germany": ["Germany", "DE", "Deutschland", "germany"],
    "Canada": ["Canada", "CA", "canada"],
    "Singapore": ["Singapore", "SG", "singapore"],
    "United Arab Emirates": ["United Arab Emirates", "UAE", "U.A.E."],
    "Japan": ["Japan", "JP", "japan"],
}
COUNTRY_WEIGHTS = [30, 15, 12, 12, 7, 7, 6, 4, 4, 3]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Cash on Delivery", "Bank Transfer"]
PAYMENT_ALIASES = {
    "Credit Card": ["Credit Card", "credit card", "CREDIT_CARD", "CC"],
    "Debit Card": ["Debit Card", "debit card", "DEBIT"],
    "PayPal": ["PayPal", "paypal", "Pay Pal"],
    "Cash on Delivery": ["Cash on Delivery", "COD", "cash"],
    "Bank Transfer": ["Bank Transfer", "bank transfer", "wire"],
}

DATE_FORMATS = [
    "%Y-%m-%d",          # 2024-03-05            (ISO, most common)
    "%Y-%m-%d",
    "%Y-%m-%d",
    "%d/%m/%Y",          # 05/03/2024            (day-first, EU/LK style)
    "%b %d %Y",          # Mar 05 2024
    "%Y/%m/%d %H:%M",    # 2024/03/05 14:22
    "%d-%b-%Y",          # 05-Mar-2024
]

START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 6, 30)

COLUMNS = [
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


def messy_case(rng: random.Random, value: str) -> str:
    """Randomly mangle casing / whitespace the way hand-typed data often is."""
    roll = rng.random()
    if roll < 0.08:
        value = value.upper()
    elif roll < 0.16:
        value = value.lower()
    if rng.random() < 0.07:
        value = f"  {value} "
    return value


def format_price(rng: random.Random, price: float) -> str:
    roll = rng.random()
    if roll < 0.10:
        return f"${price:,.2f}"          # $1,299.00
    if roll < 0.15:
        return f"{price:,.2f}"           # 1,299.00
    if roll < 0.18:
        return f"USD {price:.2f}"        # USD 24.99
    return f"{price:.2f}"


def build_clean_record(rng: random.Random, fake: Faker, seq: int, customers: list[tuple[str, str]]) -> dict:
    product = rng.choice(list(PRODUCTS))
    category, base_price = PRODUCTS[product]
    # small price variation per order (discounts / regional pricing)
    price = round(base_price * rng.uniform(0.85, 1.10), 2)
    country = rng.choices(list(COUNTRIES), weights=COUNTRY_WEIGHTS)[0]
    name, email = rng.choice(customers)

    # skew order dates so there is a visible growth trend month over month
    span = (END_DATE - START_DATE).days
    offset = int(span * (rng.random() ** 0.8))
    order_dt = datetime.combine(START_DATE + timedelta(days=offset), datetime.min.time()) + timedelta(
        hours=rng.randint(0, 23), minutes=rng.randint(0, 59)
    )

    return {
        "order_id": f"ORD-{seq:06d}",
        "customer_name": name,
        "customer_email": email,
        "product_name": product,
        "category": category,
        "quantity": rng.choices([1, 1, 1, 2, 2, 3, 4, 5], k=1)[0],
        "unit_price": price,
        "rating": rng.choices([5, 4, 4.5, 3, 3.5, 2, 1], weights=[30, 25, 15, 12, 8, 6, 4])[0],
        "country": country,
        "order_dt": order_dt,
        "payment_method": rng.choice(PAYMENT_METHODS),
    }


def dirty_record(rng: random.Random, rec: dict) -> dict:
    """Turn a clean record into a raw string row with realistic noise injected."""
    row = {
        "order_id": rec["order_id"],
        "customer_name": messy_case(rng, rec["customer_name"]),
        "customer_email": rec["customer_email"],
        "product_name": messy_case(rng, rec["product_name"]),
        "category": messy_case(rng, rec["category"]),
        "quantity": str(rec["quantity"]),
        "unit_price": format_price(rng, rec["unit_price"]),
        "rating": str(rec["rating"]),
        "country": messy_case(rng, rng.choice(COUNTRIES[rec["country"]])),
        "order_date": rec["order_dt"].strftime(rng.choice(DATE_FORMATS)),
        "payment_method": rng.choice(PAYMENT_ALIASES[rec["payment_method"]]),
    }

    # --- missing values ---
    for col, p in [
        ("rating", 0.06),
        ("country", 0.03),
        ("category", 0.03),
        ("customer_email", 0.02),
        ("unit_price", 0.01),
        ("payment_method", 0.02),
        ("product_name", 0.005),
    ]:
        if rng.random() < p:
            row[col] = rng.choice(["", "", "N/A", "null", "  "])

    # --- invalid / out-of-range values (should be rejected or fixed) ---
    if rng.random() < 0.01:
        row["quantity"] = rng.choice(["-1", "0", "-3"])
    if rng.random() < 0.005:
        row["quantity"] = rng.choice(["two", "1.5", ""])
    if rng.random() < 0.01:
        row["rating"] = rng.choice(["7", "-1", "10", "0"])
    if rng.random() < 0.005:
        row["unit_price"] = rng.choice(["-15.00", "0", "free"])
    if rng.random() < 0.01:
        row["customer_email"] = rng.choice(
            [row["customer_email"].replace("@", ""), "not-an-email", row["customer_email"] + "@@x"]
        )
    if rng.random() < 0.005:
        row["order_date"] = rng.choice(["2031-01-15", "2099-12-31", "31/02/2024", "yesterday", ""])
    if rng.random() < 0.02:
        row["customer_email"] = row["customer_email"].upper()

    return row


def generate(rows: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    # a realistic customer base: ~1 customer per 4 orders, with repeat buyers
    customers = []
    for _ in range(max(rows // 4, 50)):
        first, last = fake.first_name(), fake.last_name()
        domain = rng.choice(["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", fake.domain_name()])
        local = re.sub(r"[^a-z]", "", first.lower()) + "." + re.sub(r"[^a-z]", "", last.lower())
        email = f"{local}{rng.randint(1, 999)}@{domain}"
        customers.append((f"{first} {last}", email))

    # Leave room for injected duplicates so the final file is ~`rows` lines.
    n_exact_dupes = int(rows * 0.03)
    n_id_dupes = int(rows * 0.01)
    n_base = rows - n_exact_dupes - n_id_dupes

    out = [dirty_record(rng, build_clean_record(rng, fake, i + 1, customers)) for i in range(n_base)]

    # exact duplicates (e.g. the same export batch ingested twice)
    for _ in range(n_exact_dupes):
        out.append(dict(rng.choice(out[:n_base])))

    # same order_id but formatted differently (lower-case / whitespace) and a
    # slightly different payload, e.g. a later update of the same order
    for _ in range(n_id_dupes):
        dup = dict(rng.choice(out[:n_base]))
        dup["order_id"] = rng.choice([dup["order_id"].lower(), f" {dup['order_id']}", dup["order_id"] + " "])
        dup["customer_name"] = dup["customer_name"].upper()
        out.append(dup)

    rng.shuffle(out)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=12_000, help="number of rows to generate (default 12000)")
    parser.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    data = generate(args.rows, args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(data)

    print(f"Wrote {len(data):,} rows to {args.output}")


if __name__ == "__main__":
    main()
