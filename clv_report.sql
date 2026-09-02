-- Customer Lifetime Value (CLV) Report
-- Calculates total spending per customer, ranked by lifetime value
-- Excludes cancelled and pending orders (only COMPLETED counts)

SELECT
    c.customer_id,
    c.full_name,
    COUNT(o.order_id) AS total_orders_placed,
    ROUND(SUM(o.usd_amount), 2) AS lifetime_value_usd,
    strftime('%Y-%m', c.signup_date) AS customer_cohort
FROM dim_customers c
LEFT JOIN fct_orders o
    ON c.customer_id = o.customer_id
    AND o.status = 'COMPLETED'
GROUP BY c.customer_id, c.full_name, c.signup_date
ORDER BY lifetime_value_usd DESC;
