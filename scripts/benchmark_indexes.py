"""Benchmark the analytical queries with and without the secondary indexes.

At ~11k rows every query runs in a few milliseconds and PostgreSQL correctly
prefers sequential scans, so indexes can't show any benefit. To show their
effect realistically, this script:

1. builds an isolated `bench` schema with a fact_sales copy inflated to
   1,000,000 rows (sampled from the real cleaned data, dates spread over
   2018-2025). It shares the real dimension tables.
2. runs every query in sql/03_analytical_queries.sql N times WITHOUT the
   secondary indexes from sql/02_indexes.sql (only the PK exists)
3. creates those indexes, VACUUM ANALYZEs, and runs the queries again
4. writes plans and median timings to docs/benchmark_results.md
5. drops the bench schema (unless --keep)

    python scripts/benchmark_indexes.py [--rows 1000000] [--runs 5] [--keep]
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2  # noqa: E402

from etl.config import get_settings  # noqa: E402

QUERIES_FILE = PROJECT_ROOT / "sql" / "03_analytical_queries.sql"
INDEXES_FILE = PROJECT_ROOT / "sql" / "02_indexes.sql"
OUTPUT_FILE = PROJECT_ROOT / "docs" / "benchmark_results.md"


def load_queries() -> dict[str, str]:
    text = QUERIES_FILE.read_text()
    parts = re.split(r"^-- name: (\S+)\s*$", text, flags=re.M)
    return {parts[i]: parts[i + 1].strip().rstrip(";") for i in range(1, len(parts), 2)}


def fact_index_statements() -> list[str]:
    statements = [s.strip() for s in INDEXES_FILE.read_text().split(";")]
    cleaned = ["\n".join(l for l in s.splitlines() if not l.strip().startswith("--")).strip() for s in statements]
    return [s for s in cleaned if s.startswith("CREATE INDEX") and "ON fact_sales" in s and "etl_run" not in s]


def build_bench(cur, rows: int) -> None:
    print(f"Building bench.fact_sales with {rows:,} rows ...", flush=True)
    cur.execute("DROP SCHEMA IF EXISTS bench CASCADE")
    cur.execute("CREATE SCHEMA bench")
    cur.execute(
        "CREATE TABLE bench.fact_sales "
        "(LIKE public.fact_sales INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS)"
    )
    cur.execute(
        """
        INSERT INTO bench.fact_sales (order_id, customer_id, product_id, country_id, order_date,
                                      quantity, unit_price, rating, payment_method, etl_run_id)
        SELECT 'ORD-' || lpad(g::text, 6, '0'),
               s.customer_id, s.product_id, s.country_id,
               DATE '2018-01-01' + (random() * (DATE '2025-06-30' - DATE '2018-01-01'))::int,
               s.quantity, s.unit_price, s.rating, s.payment_method, s.etl_run_id
        FROM generate_series(0, %(rows)s - 1) AS g
        -- the join key depends only on g, so the planner can use a hash join
        JOIN (SELECT f.*, row_number() OVER () - 1 AS rn FROM public.fact_sales f) s
          ON s.rn = g %% (SELECT count(*) FROM public.fact_sales)
        """,
        {"rows": rows},
    )
    cur.execute("ALTER TABLE bench.fact_sales ADD PRIMARY KEY (order_id)")


def vacuum_analyze(conn) -> None:
    old = conn.autocommit
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE bench.fact_sales")  # also sets the visibility map -> index-only scans
    conn.autocommit = old


def scan_nodes(plan: dict) -> list[str]:
    out = []
    node = plan.get("Node Type", "")
    if "Scan" in node and plan.get("Relation Name") == "fact_sales":
        label = node
        if plan.get("Index Name"):
            label += f" using {plan['Index Name']}"
        out.append(label)
    for child in plan.get("Plans", []):
        out.extend(scan_nodes(child))
    return out


def run_query(cur, sql: str, runs: int) -> tuple[float, int, list[str], str]:
    times = []
    for _ in range(runs):
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)
        plan = cur.fetchone()[0][0]
        times.append(plan["Execution Time"])
    cur.execute("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY ON) " + sql)
    text_plan = "\n".join(r[0] for r in cur.fetchall())
    # buffers are cumulative at the root node: pages touched = work done, independent of cache/CPU noise
    pages = plan["Plan"].get("Shared Hit Blocks", 0) + plan["Plan"].get("Shared Read Blocks", 0)
    return statistics.median(times), pages, scan_nodes(plan["Plan"]), text_plan


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--runs", type=int, default=7, help="executions per query (median is reported)")
    parser.add_argument("--keep", action="store_true", help="keep the bench schema afterwards")
    args = parser.parse_args()
    if args.rows > 1_000_000:
        parser.error("--rows max is 1,000,000 (order_id format is ORD-NNNNNN)")

    settings = get_settings()
    queries = load_queries()
    index_sql = fact_index_statements()

    conn = psycopg2.connect(**settings.postgres.dsn())
    conn.autocommit = False
    results: dict[str, dict] = {}
    try:
        with conn.cursor() as cur:
            t0 = time.perf_counter()
            build_bench(cur, args.rows)
            conn.commit()
            vacuum_analyze(conn)
            print(f"  built in {time.perf_counter() - t0:.1f}s")

            cur.execute("SET search_path = bench, public")
            cur.execute("SELECT pg_size_pretty(pg_total_relation_size('bench.fact_sales'))")
            size_before = cur.fetchone()[0]

            print("Running queries WITHOUT secondary indexes ...")
            for name, sql in queries.items():
                ms, pages, nodes, plan = run_query(cur, sql, args.runs)
                results[name] = {"before_ms": ms, "before_pages": pages, "before_nodes": nodes, "before_plan": plan}
                print(f"  {name:<36} {ms:9.2f} ms {pages:>8,} pages  {nodes}")

            print("Creating indexes from sql/02_indexes.sql ...")
            for stmt in index_sql:
                cur.execute(stmt)
            conn.commit()
            vacuum_analyze(conn)
            cur.execute("SET search_path = bench, public")
            cur.execute("SELECT pg_size_pretty(pg_total_relation_size('bench.fact_sales'))")
            size_after = cur.fetchone()[0]

            print("Running queries WITH indexes ...")
            for name, sql in queries.items():
                ms, pages, nodes, plan = run_query(cur, sql, args.runs)
                results[name].update(after_ms=ms, after_pages=pages, after_nodes=nodes, after_plan=plan)
                print(f"  {name:<36} {ms:9.2f} ms {pages:>8,} pages  {nodes}")

            cur.execute("SELECT version()")
            pg_version = cur.fetchone()[0].split(",")[0]
    finally:
        conn.rollback()
        if not args.keep:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS bench CASCADE")
            conn.commit()
        conn.close()

    write_report(results, args, size_before, size_after, pg_version, index_sql)
    print(f"\nReport written to {OUTPUT_FILE.relative_to(PROJECT_ROOT)}")


def write_report(results, args, size_before, size_after, pg_version, index_sql) -> None:
    lines = [
        "# Index benchmark results",
        "",
        f"_Generated by `scripts/benchmark_indexes.py` on {datetime.now():%Y-%m-%d %H:%M}._",
        "",
        f"- **Data:** `bench.fact_sales` with **{args.rows:,} rows** (sampled from the cleaned data, dates spread 2018 to mid-2025)",
        f"- **Engine:** {pg_version} (Docker, default config)",
        f"- **Method:** `EXPLAIN (ANALYZE, BUFFERS)`, median of {args.runs} runs, warm cache",
        f"- **Table + index size:** {size_before} (PK only) to {size_after} (with secondary indexes)",
        "",
        "Pages = 8 KB buffer pages touched (shared hit + read). This is the I/O work the query does,",
        "and it is far less noisy than wall-clock time on a laptop.",
        "",
        "| Query | Time without | Time with | Speed-up | Pages without | Pages with | Page reduction | Plan without | Plan with |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for name, r in results.items():
        speedup = r["before_ms"] / r["after_ms"] if r["after_ms"] else float("inf")
        page_cut = 100 * (1 - r["after_pages"] / r["before_pages"]) if r["before_pages"] else 0
        lines.append(
            f"| `{name}` | {r['before_ms']:.1f} ms | {r['after_ms']:.1f} ms | **{speedup:.1f}x** | "
            f"{r['before_pages']:,} | {r['after_pages']:,} | {page_cut:.0f}% | "
            f"{', '.join(dict.fromkeys(r['before_nodes'])) or '-'} | {', '.join(dict.fromkeys(r['after_nodes'])) or '-'} |"
        )
    lines += ["", "## Indexes under test", "", "```sql", ";\n\n".join(index_sql) + ";", "```", "", "## Query plans", ""]
    for name, r in results.items():
        lines += [
            f"### {name}",
            "",
            "<details><summary>Without secondary indexes</summary>",
            "",
            "```",
            r["before_plan"],
            "```",
            "</details>",
            "",
            "<details><summary>With indexes</summary>",
            "",
            "```",
            r["after_plan"],
            "```",
            "</details>",
            "",
        ]
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text("\n".join(lines))
    (OUTPUT_FILE.with_suffix(".json")).write_text(
        json.dumps({k: {kk: v for kk, v in r.items() if "plan" not in kk} for k, r in results.items()}, indent=2)
    )


if __name__ == "__main__":
    main()
