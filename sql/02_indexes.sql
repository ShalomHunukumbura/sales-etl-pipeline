CREATE INDEX IF NOT EXISTS idx_fact_sales_date_covering
    ON fact_sales (order_date)
    INCLUDE (product_id, country_id, quantity, total_amount, rating);


CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_date
    ON fact_sales (customer_id, order_date DESC)
    INCLUDE (total_amount);


CREATE INDEX IF NOT EXISTS idx_fact_sales_etl_run
    ON fact_sales (etl_run_id);

CREATE INDEX IF NOT EXISTS idx_rejected_records_run
    ON rejected_records (run_id);
