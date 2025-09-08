SELECT user_id, COUNT(order_id) AS orders_90d, SUM(order_value) AS revenue_90d,
AVG(DATE_DIFF(order_ts, LAG(order_ts) OVER (PARTITION BY user_id ORDER BY order_ts), DAY)) AS avg_days_between_orders
FROM {{ ref('orders') }} WHERE order_ts>=DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY user_id
