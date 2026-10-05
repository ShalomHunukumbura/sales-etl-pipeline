-- =====================================================================
--  Indexes
--  Primary keys and UNIQUE constraints already create B-tree indexes on
--  order_id, email, product_name and country_name. The indexes below are
--  chosen for the access paths in 03_analytical_queries.sql and measured
--  with scripts/benchmark_indexes.py (results: docs/benchmark_results.md).
--  See README "Optimization decisions" for the reasoning.
-- =====================================================================

-- 1. Date range is the dominant predicate of every reporting query
--    ("last 12 months", "this financial year"). One covering index keyed on
--    order_date that INCLUDEs every column those reports read lets Q1, Q2 and
--    Q3 run as index-only scans: they read only the matching slice of the
--    index and never touch the table heap.
--    INCLUDE columns are stored in the leaf pages only, so they don't widen
--    the search key or affect ordering.
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_covering
    ON fact_sales (order_date)
    INCLUDE (product_id, country_id, quantity, total_amount, rating);

-- 2. Customer access path: order history lookups (Q5) by customer, newest
--    first, and lifetime value (Q4). Also serves as the index for the
--    customer_id foreign key.
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_date
    ON fact_sales (customer_id, order_date DESC)
    INCLUDE (total_amount);

-- 3. Operational: audit or roll back everything a given ETL run wrote
--    (DELETE FROM fact_sales WHERE etl_run_id = ...).
CREATE INDEX IF NOT EXISTS idx_fact_sales_etl_run
    ON fact_sales (etl_run_id);

CREATE INDEX IF NOT EXISTS idx_rejected_records_run
    ON rejected_records (run_id);

-- Deliberately NOT created:
--   * standalone indexes on product_id / country_id: the dimensions are tiny
--     (28 and 11 rows) and never deleted, so FK-check indexes buy nothing.
--     The covering date index already carries both columns for reports.
--   * an index on dim_product(category): a 28-row table is always scanned
--     sequentially. Indexing it would only add maintenance cost.
--   Every extra index slows down the bulk load and takes disk space, so each
--   one has to earn its place with a query it measurably speeds up.
