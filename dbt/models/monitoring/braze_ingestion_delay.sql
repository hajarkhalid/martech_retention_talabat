SELECT MAX(event_timestamp) AS last_event_ts, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(event_timestamp), HOUR) AS hours_delay
FROM {{ ref('braze_currents') }}
