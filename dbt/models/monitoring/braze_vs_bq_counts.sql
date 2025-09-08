WITH braze AS (SELECT DATE(event_timestamp) AS day, COUNT(*) AS braze_count FROM {{ ref('braze_currents') }} WHERE DATE(event_timestamp)=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) GROUP BY day),
bq AS (SELECT DATE(event_timestamp) AS day, COUNT(*) AS bq_count FROM {{ ref('events') }} WHERE DATE(event_timestamp)=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) GROUP BY day)
SELECT b.day, braze_count, bq_count, SAFE_DIVIDE(ABS(braze_count-bq_count), GREATEST(braze_count,bq_count))*100 AS variance_pct FROM braze b JOIN bq USING(day)
