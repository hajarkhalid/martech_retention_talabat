from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'martech',
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG('talabat_retention_daily', start_date=datetime(2025,1,1), schedule_interval='@daily', catchup=False, default_args=default_args) as dag:
    run_dbt_daily = BashOperator(
        task_id='dbt_build_daily',
        bash_command='cd /opt/project && dbt deps && dbt build --select tag:daily',
    )

    upload_cc = PythonOperator(
        task_id='upload_connected_content',
        python_callable=lambda: __import__('scripts.upload_connected_content').upload_content()
    )

    trigger_braze = PythonOperator(
        task_id='trigger_braze_campaign',
        python_callable=lambda: __import__('scripts.braze_sync').trigger_braze_campaign_if_ready()
    )

    run_dbt_daily >> upload_cc >> trigger_braze
