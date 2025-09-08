-- measure % of churned users reactivated after campaign
SELECT c.campaign_id, COUNT(DISTINCT u.user_id) AS treated, COUNT(DISTINCT r.user_id) AS reactivated,
SAFE_DIVIDE(COUNT(DISTINCT r.user_id), NULLIF(COUNT(DISTINCT u.user_id),0)) AS reactivation_rate
FROM {{ ref('campaign_events') }} c
LEFT JOIN {{ ref('users') }} u ON c.user_id = u.user_id
LEFT JOIN {{ ref('orders') }} r ON r.user_id = u.user_id AND DATE_DIFF(r.order_ts, c.sent_at, DAY) BETWEEN 1 AND 30
WHERE c.event_name='send' AND c.campaign_type='winback'
GROUP BY c.campaign_id
