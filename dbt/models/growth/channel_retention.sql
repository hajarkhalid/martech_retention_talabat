-- retention by acquisition channel
WITH acq AS (SELECT user_id, acquisition_channel FROM {{ ref('users') }})
, activity AS (SELECT user_id, DATE(order_ts) AS day FROM {{ ref('orders') }})
SELECT acq.acquisition_channel, DATE_TRUNC(day, MONTH) AS month, COUNT(DISTINCT acq.user_id) AS active_users
FROM acq JOIN activity USING(user_id)
GROUP BY 1,2
ORDER BY 1,2 DESC
