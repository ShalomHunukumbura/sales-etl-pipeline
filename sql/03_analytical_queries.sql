-- =====================================================================
--  Analytical queries
--  Run:  docker exec -i sales_etl_postgres psql -U etl_user -d sales_dw < sql/03_analytical_queries.sql
--  Each query is tagged "-- name: <id>" so scripts/benchmark_indexes.py can
--  run it with and without indexes.
-- =====================================================================

-- name: q1_top_categories_by_revenue
-- Top 10 categories by revenue for the last financial year (Jul 2024 - Jun 2025).
-- Uses: idx_fact_sales_date_covering -> index-only scan of just the date range
SELECT p.category,
       COUNT(*)                                   AS orders,
       SUM(f.quantity)                            AS units_sold,
       SUM(f.total_amount)                        AS revenue,
       ROUND(100.0 * SUM(f.total_amount)
             / SUM(SUM(f.total_amount)) OVER (), 2) AS revenue_share_pct
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
WHERE f.order_date >= DATE '2024-07-01'
  AND f.order_date <  DATE '2025-07-01'
GROUP BY p.category
ORDER BY revenue DESC
LIMIT 10;

-- name: q2_monthly_revenue_growth
-- Month-over-month revenue growth for the last 12 months.
-- Uses: idx_fact_sales_date_covering -> index-only scan
WITH monthly AS (
    -- cast to timestamp first: date_trunc(date) silently promotes to timestamptz,
    -- which does a time-zone conversion per row (~40% slower in the benchmark)
    SELECT date_trunc('month', order_date::timestamp)::date AS month,
           COUNT(*)                              AS orders,
           SUM(total_amount)                     AS revenue
    FROM fact_sales
    WHERE order_date >= DATE '2024-07-01'
      AND order_date <  DATE '2025-07-01'
    GROUP BY 1
)
SELECT month,
       orders,
       revenue,
       LAG(revenue) OVER (ORDER BY month)                         AS prev_month_revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
             / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 2)  AS mom_growth_pct
FROM monthly
ORDER BY month;

-- name: q3_avg_rating_by_country
-- Average customer rating by country for the last financial year
-- (countries with at least 30 ratings, so small samples don't mislead).
-- Uses: idx_fact_sales_date_covering (carries country_id + rating) -> index-only scan
SELECT c.country_name,
       COUNT(f.rating)            AS ratings,
       ROUND(AVG(f.rating), 2)    AS avg_rating,
       ROUND(100.0 * COUNT(*) FILTER (WHERE f.rating >= 4) / NULLIF(COUNT(f.rating), 0), 1)
                                  AS pct_4_plus
FROM fact_sales f
JOIN dim_country c ON c.country_id = f.country_id
WHERE f.order_date >= DATE '2024-07-01'
  AND f.order_date <  DATE '2025-07-01'
GROUP BY c.country_name
HAVING COUNT(f.rating) >= 30
ORDER BY avg_rating DESC;

-- name: q4_top_customers_lifetime_value
-- Top 10 customers by lifetime value (all time).
-- Full-table aggregate: a sequential scan is the correct plan (see README)
SELECT cu.customer_name,
       cu.email,
       COUNT(*)              AS orders,
       SUM(f.total_amount)   AS lifetime_value,
       MIN(f.order_date)     AS first_order,
       MAX(f.order_date)     AS last_order
FROM fact_sales f
JOIN dim_customer cu ON cu.customer_id = f.customer_id
GROUP BY cu.customer_id, cu.customer_name, cu.email
ORDER BY lifetime_value DESC
LIMIT 10;

-- name: q5_customer_order_history
-- Operational lookup: one customer's order history (e.g. a support / CRM screen).
-- Uses: idx_fact_sales_customer_date -> reads ~18 pages instead of the whole table
SELECT f.order_id, f.order_date, p.product_name, f.quantity, f.total_amount
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
WHERE f.customer_id = 42
ORDER BY f.order_date DESC;
