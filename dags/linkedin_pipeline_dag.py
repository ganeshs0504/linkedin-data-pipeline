import os
from scripts import sandbox

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='branch_operator_dag',
    default_args=default_args,
    description='Something to say about this dag',
    # start_date=datetime(2024, 10, 22),
    start_date=datetime.today(),
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

    upload_or_skip_branch_task = BranchPythonOperator(
        task_id='upload_or_skip_branch_task',
        python_callable=sandbox.check_if_file_exists_in_gcs
    )

    upload_file_to_gcs = LocalFilesystemToGCSOperator(
        gcp_conn_id='gcp_creds',
        task_id='upload_file_to_gcs',
        src='raw_data/downloaded_data/output.csv',
        dst='downloaded_data/output.csv',
        bucket='test_bucket_airflow_99'
    )

    processing_placeholder = BashOperator(
        task_id='bash_placeholder',
        bash_command="echo FILE ALREADY EXISTS SO SKIPPING TO PROCESSING"
    )



    download_and_extract_task >> [print_debug_data, upload_or_skip_branch_task]
    upload_or_skip_branch_task >> [upload_file_to_gcs, processing_placeholder]