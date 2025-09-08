from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {'owner':'martech','retries':1,'retry_delay':timedelta(minutes=5)}

with DAG('talabat_monitoring', start_date=datetime(2025,1,1), schedule_interval='@hourly', catchup=False, default_args=default_args) as dag:
    check_freshness = PythonOperator(task_id='check_data_freshness', python_callable=lambda: __import__('scripts.slack_alerts').check_and_alert())
