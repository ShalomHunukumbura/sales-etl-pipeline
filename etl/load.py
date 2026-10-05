from __future__ import annotations

import io
import logging
from contextlib import contextmanager
from typing import Iterator

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values

from etl.config import PostgresConfig

log = logging.getLogger("load")

STAGING_COLUMNS = [
    "order_id", "customer_name", "customer_email", "product_name", "category",
    "quantity", "unit_price", "rating", "country", "order_date", "payment_method",
]

MERGE_DIMENSIONS_SQL = """
INSERT INTO dim_country (country_name)
SELECT DISTINCT country FROM staging_sales
ON CONFLICT (country_name) DO NOTHING;

-- a product's category is the most frequent one seen for it
INSERT INTO dim_product (product_name, category)
SELECT product_name, mode() WITHIN GROUP (ORDER BY category)
FROM staging_sales
GROUP BY product_name
ON CONFLICT (product_name) DO UPDATE
    SET category = EXCLUDED.category
    WHERE dim_product.category IS DISTINCT FROM EXCLUDED.category;

-- a customer's display name is the most frequent spelling seen for the email
INSERT INTO dim_customer (email, customer_name)
SELECT customer_email, mode() WITHIN GROUP (ORDER BY customer_name)
FROM staging_sales
WHERE customer_email IS NOT NULL
GROUP BY customer_email
ON CONFLICT (email) DO UPDATE
    SET customer_name = EXCLUDED.customer_name
    WHERE dim_customer.customer_name IS DISTINCT FROM EXCLUDED.customer_name;
"""

MERGE_FACT_SQL = """
INSERT INTO fact_sales AS f (
    order_id, customer_id, product_id, country_id, order_date,
    quantity, unit_price, rating, payment_method, etl_run_id
)
SELECT s.order_id, c.customer_id, p.product_id, co.country_id, s.order_date,
       s.quantity, s.unit_price, s.rating, s.payment_method, %(run_id)s
FROM staging_sales s
JOIN      dim_product  p  ON p.product_name  = s.product_name
JOIN      dim_country  co ON co.country_name = s.country
LEFT JOIN dim_customer c  ON c.email         = s.customer_email
ON CONFLICT (order_id) DO UPDATE SET
    customer_id    = EXCLUDED.customer_id,
    product_id     = EXCLUDED.product_id,
    country_id     = EXCLUDED.country_id,
    order_date     = EXCLUDED.order_date,
    quantity       = EXCLUDED.quantity,
    unit_price     = EXCLUDED.unit_price,
    rating         = EXCLUDED.rating,
    payment_method = EXCLUDED.payment_method,
    etl_run_id     = EXCLUDED.etl_run_id,
    loaded_at      = now()
WHERE (f.customer_id, f.product_id, f.country_id, f.order_date,
       f.quantity, f.unit_price, f.rating, f.payment_method)
      IS DISTINCT FROM
      (EXCLUDED.customer_id, EXCLUDED.product_id, EXCLUDED.country_id, EXCLUDED.order_date,
       EXCLUDED.quantity, EXCLUDED.unit_price, EXCLUDED.rating, EXCLUDED.payment_method)
RETURNING (xmax = 0) AS inserted;   -- xmax = 0 -> row was inserted, not updated
"""


@contextmanager
def connect(cfg: PostgresConfig) -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(**cfg.dsn(), connect_timeout=10, application_name="sales_etl")
    try:
        yield conn
    finally:
        conn.close()


def start_run(conn, run_id: str, source_file: str) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO etl_runs (run_id, source_file, status) VALUES (%s, %s, 'running')",
            (run_id, source_file),
        )


def finish_run(conn, run_id: str, status: str, stats: dict, error: str | None = None) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_runs
               SET finished_at = now(), status = %s, error_message = %s,
                   rows_extracted = %s, rows_duplicates = %s, rows_rejected = %s,
                   rows_inserted = %s, rows_updated = %s
             WHERE run_id = %s
            """,
            (
                status, error,
                stats.get("extracted"), stats.get("duplicates"), stats.get("rejected"),
                stats.get("inserted"), stats.get("updated"), run_id,
            ),
        )


def _copy_to_staging(cur, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df[STAGING_COLUMNS].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cur.copy_expert(
        f"COPY staging_sales ({', '.join(STAGING_COLUMNS)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
        buf,
    )


def load_sales(conn, df: pd.DataFrame, run_id: str) -> tuple[int, int]:
    """Load the validated frame. Returns (inserted, updated)."""
    with conn, conn.cursor() as cur:  # single transaction: commit on success, rollback on error
        cur.execute("TRUNCATE staging_sales")
        _copy_to_staging(cur, df)
        log.info("COPY -> staging_sales: %s rows", f"{len(df):,}")

        cur.execute(MERGE_DIMENSIONS_SQL)
        cur.execute(MERGE_FACT_SQL, {"run_id": run_id})
        flags = [row[0] for row in cur.fetchall()]
        inserted, updated = sum(flags), len(flags) - sum(flags)

        cur.execute("TRUNCATE staging_sales")
        cur.execute("ANALYZE fact_sales")  # keep planner statistics fresh after a bulk load

    log.info("Merged into fact_sales: %s inserted, %s updated, %s unchanged",
             f"{inserted:,}", f"{updated:,}", f"{len(df) - inserted - updated:,}")
    return inserted, updated


def load_rejects(conn, rejects: pd.DataFrame, raw: pd.DataFrame, run_id: str) -> None:
    """Write rejected rows, with the original untouched record, to rejected_records."""
    if rejects.empty:
        return
    raw_by_row = raw.set_index("source_row")
    rows = [
        (run_id, int(r.source_row), r.reject_reason, Json(raw_by_row.loc[r.source_row].to_dict()))
        for r in rejects.itertuples(index=False)
    ]
    with conn, conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO rejected_records (run_id, source_row, reject_reason, raw_record) VALUES %s",
            rows,
            page_size=1000,
        )
    log.info("Logged %s rejected records to rejected_records", f"{len(rows):,}")
