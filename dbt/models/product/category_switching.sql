-- track users switching top categories between first and recent order
WITH first_cat AS (SELECT user_id, ARRAY_AGG(category ORDER BY order_ts LIMIT 1)[OFFSET(0)] AS first_cat FROM {{ ref('orders') }} GROUP BY user_id),
recent_cat AS (SELECT user_id, ARRAY_AGG(category ORDER BY order_ts DESC LIMIT 1)[OFFSET(0)] AS recent_cat FROM {{ ref('orders') }} GROUP BY user_id)
SELECT f.first_cat, r.recent_cat, COUNT(*) AS users FROM first_cat f JOIN recent_cat r USING(user_id) GROUP BY 1,2 ORDER BY users DESC
