# Architecture Overview
Data sources -> Raw BigQuery -> dbt (staging + analytics + features) -> Airflow orchestration -> ML notebooks -> Predictions -> Braze activation (Connected Content) -> Performance back to BigQuery.
