SELECT u.user_id, DATE(u.signup_date) AS signup_date,
COALESCE(SUM(o.order_value),0) AS total_spend, COALESCE(COUNT(o.order_id),0) AS total_orders,
SAFE_DIVIDE(COALESCE(SUM(o.order_value),0), NULLIF(COALESCE(COUNT(o.order_id),0),0)) AS avg_order_value,
CASE WHEN COALESCE(MAX(o.order_ts),'1970-01-01') < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 ELSE 0 END AS churn_label
FROM {{ ref('users') }} u LEFT JOIN {{ ref('orders') }} o USING(user_id)
GROUP BY u.user_id, u.signup_date
