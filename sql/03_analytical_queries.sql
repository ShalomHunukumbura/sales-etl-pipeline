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


WITH monthly AS (
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

SELECT f.order_id, f.order_date, p.product_name, f.quantity, f.total_amount
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
WHERE f.customer_id = 42
ORDER BY f.order_date DESC;
