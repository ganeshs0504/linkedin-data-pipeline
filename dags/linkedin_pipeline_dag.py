import os
from scripts import sandbox

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='some_unique_id56',
    default_args=default_args,
    description='Something to say about this dag',
    start_date=datetime(2024, 10, 22),
    schedule_interval='@daily'
) as dag:
    download_and_extract_task = PythonOperator(
        task_id='download_and_extract',
        python_callable=sandbox.download_and_extract_raw_data_callable
    )

    print_debug_data = PythonOperator(
        task_id='print_downloaded_data',
        python_callable=sandbox.print_loaded_csv_callable
    )

    download_and_extract_task >> print_debug_data