-- cross vertical usage (food, grocery, pharmacy)
SELECT user_id, COUNT(DISTINCT vertical) AS verticals_used, ARRAY_AGG(DISTINCT vertical) AS verticals
FROM {{ ref('orders') }}
GROUP BY user_id
