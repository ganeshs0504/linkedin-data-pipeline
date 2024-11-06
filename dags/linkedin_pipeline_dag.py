import os
from scripts import sandbox
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.dummy import DummyOperator

with open("./config.json", 'r') as f:
    config = json.load(f)

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='linkedin_datapipeline_dag_test_final',
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

    upload_or_skip_branch_task = BranchPythonOperator(
        task_id='upload_or_skip_branch_task',
        python_callable=sandbox.check_if_file_exists_in_gcs
    )

    upload_folder_to_gcs = PythonOperator(
        task_id='upload_folder_to_gcs',
        python_callable=sandbox.upload_folder_to_gcs,
        provide_context=True
    )

    upload_dataproc_job_csv_to_parquet = LocalFilesystemToGCSOperator(
        gcp_conn_id=config["airflow_gcp_conn_id"],
        task_id='upload_dataproc_job_csv_to_parquet',
        src='dags/scripts/csv_to_parquet_pyspark.py',
        dst='csv_to_parquet_pyspark.py',
        bucket=config["dataproc_jobs_bucket_name"]
    )

    upload_dataproc_job_transform_to_bq = LocalFilesystemToGCSOperator(
        gcp_conn_id=config["airflow_gcp_conn_id"],
        task_id='upload_dataproc_job_transform_to_bq',
        src='dags/scripts/data_transformation_pyspark.py',
        dst='data_transformation_pyspark.py',
        bucket=config["dataproc_jobs_bucket_name"]
    )

    skip_upload_dummy = DummyOperator(task_id='skip_upload_dummy')

    submit_csv_to_parquet_dataproc_job = DataprocSubmitJobOperator(
        task_id='submit_csv_to_parquet_dataproc_job',
        job={
            "placement": {"cluster_name": config["dataproc_cluster_name"]},
            "pyspark_job": {"main_python_file_uri": f"gs://{config['dataproc_jobs_bucket_name']}/csv_to_parquet_pyspark.py"}
        },
        region="europe-west2",
        trigger_rule='all_done',
        gcp_conn_id=config["airflow_gcp_conn_id"]
    )

    transform_data_to_bq_dataproc_job = DataprocSubmitJobOperator(
        task_id='transform_data_to_bq_dataproc_job',
        job={
            "placement": {"cluster_name": config["dataproc_cluster_name"]},
            "pyspark_job": {"main_python_file_uri": f"gs://{config['dataproc_jobs_bucket_name']}/data_transformation_pyspark.py",
                            "jar_file_uris": ['gs://spark-lib/bigquery/spark-3.1-bigquery-0.41.0.jar']
            }
        },
        region="europe-west2",
        gcp_conn_id=config["airflow_gcp_conn_id"]
    )



    download_and_extract_task >> [upload_or_skip_branch_task, upload_dataproc_job_csv_to_parquet, upload_dataproc_job_transform_to_bq]
    upload_or_skip_branch_task >> [upload_folder_to_gcs, skip_upload_dummy]
    upload_folder_to_gcs >> submit_csv_to_parquet_dataproc_job
    skip_upload_dummy >> submit_csv_to_parquet_dataproc_job
    submit_csv_to_parquet_dataproc_job >> transform_data_to_bq_dataproc_job