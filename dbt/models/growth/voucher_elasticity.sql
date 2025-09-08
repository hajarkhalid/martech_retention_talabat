-- daily voucher spend and revenue for elasticity analysis
SELECT DATE(order_ts) AS day, SUM(order_value) AS revenue, SUM(IFNULL(voucher_discount,0)) AS voucher_spend
FROM {{ ref('orders') }}
GROUP BY day
ORDER BY day DESC
