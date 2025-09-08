-- silent churn detection (no orders in last 30d)
SELECT user_id, MAX(order_ts) AS last_order_ts,
CASE WHEN DATE_DIFF(CURRENT_DATE(), MAX(order_ts), DAY) > 30 THEN 'silent_churn' ELSE 'active' END AS churn_status
FROM {{ ref('orders') }}
GROUP BY user_id
