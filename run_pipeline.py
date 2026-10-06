from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from etl.config import PROCESSED_DIR, PROJECT_ROOT, RAW_DIR, REJECTED_DIR, get_settings
from etl.extract import extract
from etl.load import connect, finish_run, load, start_run
from etl.logger import setup_logging
from etl.s3 import S3Store
from etl.transform import deduplicate, standardize
from etl.validate import failure_reasons, validate

log = logging.getLogger("pipeline")


@contextmanager
def stage(name: str):
    log.info("── %s ──", name)
    t0 = time.perf_counter()
    yield
    log.info("   %s done in %.2fs", name, time.perf_counter() - t0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sales ETL pipeline")
    parser.add_argument("--input", type=Path, default=RAW_DIR / "sales_raw.csv", help="raw CSV to process")
    parser.add_argument("--generate", action="store_true", help="generate the raw dataset before running")
    parser.add_argument("--skip-s3", action="store_true", help="disable S3 uploads for this run")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    run_id = str(uuid.uuid4())
    log_file = setup_logging(run_id, settings.log_level)
    s3_on = settings.s3.enabled and not args.skip_s3

    log.info("Run %s | input=%s | S3=%s", run_id, args.input, "on" if s3_on else "off")

    if args.generate:
        subprocess.run([sys.executable, str(PROJECT_ROOT / "scripts" / "generate_data.py"), "--output", str(args.input)], check=True)

    stats: dict[str, int] = {}
    warehouse_committed = False
    t_start = time.perf_counter()

    with connect(settings.postgres) as conn:
        start_run(conn, run_id, args.input.name)
        try:
            s3 = S3Store(settings.s3, run_id) if s3_on else None

            with stage("EXTRACT"):
                raw = extract(args.input)
                stats["extracted"] = len(raw)
                if s3:
                    s3.upload(args.input, "raw")

            with stage("TRANSFORM"):
                standardized = standardize(raw)
                # tell dedup which rows would pass validation, so a valid copy of an
                # order_id is kept over an earlier invalid one
                deduped, duplicates = deduplicate(raw, standardized, valid=failure_reasons(standardized) == "")
                stats["duplicates"] = len(duplicates)

            with stage("VALIDATE"):
                valid, invalid = validate(deduped)
                stats["rejected"] = len(invalid)

            with stage("WRITE FILES"):
                PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
                REJECTED_DIR.mkdir(parents=True, exist_ok=True)
                clean_parquet = PROCESSED_DIR / "sales_clean.parquet"
                clean_csv = PROCESSED_DIR / "sales_clean.csv"
                rejected_csv = REJECTED_DIR / f"rejected_{run_id}.csv"

                out = valid.drop(columns=["source_row"])
                out.to_parquet(clean_parquet, index=False)
                out.to_csv(clean_csv, index=False)

                all_rejects = pd.concat([duplicates, invalid]).sort_values("source_row")
                all_rejects.merge(raw, on="source_row").to_csv(rejected_csv, index=False)
                log.info("Wrote %s, %s, %s", clean_parquet.name, clean_csv.name, rejected_csv.name)

            with stage("LOAD POSTGRES"):
                stats["inserted"], stats["updated"] = load(conn, valid, all_rejects, raw, run_id)
                warehouse_committed = True

            if s3:
                with stage("S3 BACKUP"):
                    s3.upload(clean_parquet, "processed")
                    s3.upload(clean_csv, "processed")
                    s3.upload(rejected_csv, "rejected", "rejected.csv")
                    objects = s3.list_run_objects()
                    log.info("Verified %s objects in S3 for this run", len(objects))

        except Exception as exc:
            conn.rollback()
            finish_run(conn, run_id, "failed", stats, error=f"{type(exc).__name__}: {exc}")
            if warehouse_committed:
                log.exception(
                    "Pipeline FAILED (run %s) AFTER the warehouse load was committed: fact_sales and "
                    "rejected_records hold this run's rows (etl_run_id = %s). Fix the cause and re-run; "
                    "the load is idempotent.", run_id, run_id,
                )
            else:
                log.exception("Pipeline FAILED (run %s). Nothing was committed to fact_sales.", run_id)
            return 1

        finish_run(conn, run_id, "success", stats)

    elapsed = time.perf_counter() - t_start
    unchanged = len(valid) - stats["inserted"] - stats["updated"]
    log.info("=" * 60)
    log.info("RUN SUMMARY  %s", run_id)
    log.info("  extracted          %8s", f"{stats['extracted']:,}")
    log.info("  duplicates removed %8s", f"{stats['duplicates']:,}")
    log.info("  rejected (invalid) %8s", f"{stats['rejected']:,}")
    log.info("  clean rows         %8s", f"{len(valid):,}")
    log.info("  inserted / updated / unchanged  %s / %s / %s",
             f"{stats['inserted']:,}", f"{stats['updated']:,}", f"{unchanged:,}")
    log.info("  elapsed            %7.2fs", elapsed)
    log.info("  log file           %s", log_file.relative_to(Path.cwd()) if log_file.is_relative_to(Path.cwd()) else log_file)
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
