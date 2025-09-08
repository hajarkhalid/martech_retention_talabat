-- onboarding funnel: app_install -> signup -> first_order
WITH installs AS (SELECT user_id, MIN(event_ts) AS install_ts FROM {{ ref('events') }} WHERE event_name='app_install' GROUP BY user_id),
signups AS (SELECT user_id, MIN(created_at) AS signup_ts FROM {{ ref('users') }} GROUP BY user_id),
first_orders AS (SELECT user_id, MIN(order_ts) AS first_order_ts FROM {{ ref('orders') }} GROUP BY user_id)
SELECT
  i.user_id,
  i.install_ts,
  s.signup_ts,
  f.first_order_ts,
  CASE WHEN s.signup_ts IS NOT NULL THEN 1 ELSE 0 END AS converted_signup,
  CASE WHEN f.first_order_ts IS NOT NULL THEN 1 ELSE 0 END AS converted_order
FROM installs i
LEFT JOIN signups s USING(user_id)
LEFT JOIN first_orders f USING(user_id)
