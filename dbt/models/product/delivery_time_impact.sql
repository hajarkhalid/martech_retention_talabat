-- Does delivery time correlate with next-order probability?
WITH orders_enh AS (
  SELECT user_id, order_id, order_ts, delivery_minutes,
         LEAD(order_ts) OVER (PARTITION BY user_id ORDER BY order_ts) AS next_order_ts
  FROM {{ ref('orders') }}
)
SELECT
  delivery_minutes_bucket,
  SAFE_DIVIDE(COUNT(CASE WHEN next_order_ts IS NOT NULL THEN 1 END), NULLIF(COUNT(order_id),0)) AS next_order_rate
FROM (
  SELECT *, CASE WHEN delivery_minutes<=20 THEN '0-20' WHEN delivery_minutes<=35 THEN '21-35' ELSE '36+' END AS delivery_minutes_bucket FROM orders_enh
)
GROUP BY delivery_minutes_bucket
