import os
import zipfile
import subprocess
import logging

from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import json

from google.cloud import storage

with open("./config.json", 'r') as f:
    config = json.load(f)


def download_and_extract_raw_data_callable():
    kaggle_conn = BaseHook.get_connection('kaggle_api')
    kaggle_creds = json.loads(kaggle_conn.extra)
    os.environ['KAGGLE_USERNAME'] = kaggle_creds['username']
    os.environ['KAGGLE_KEY'] = kaggle_creds['key']

    downloaded_data_path = config["local_dataset_download_path"]

    if os.path.exists(downloaded_data_path):
        logging.warning("FILE ALREADY DOWNLOADED")
        pass
    else:
        try:
            subprocess.run(['kaggle', 'datasets', 'download', f"{config["kaggle_dataset_id"]}"], check=True)
        except Exception as e:
            logging.error(f"Failed to download dataset: {e}")
        with zipfile.ZipFile(f"{config["kaggle_dataset_name"]}.zip", 'r') as zip_ref:
            zip_ref.extractall(downloaded_data_path)

def check_if_file_exists_in_gcs():
    name = 'downloaded_data/job_skills.csv'
    storage_client = storage.Client.from_service_account_json('/keys/gcp_creds.json')
    bucket_name = config["linkedin_data_bucket_name"]
    bucket = storage_client.bucket(bucket_name)
    stats = storage.Blob(bucket=bucket, name=name).exists(storage_client)
    logging.info("~~~~~~FILE EXISTS?~~~~~~~~~~~~~~~~~~~~~~~~")
    logging.info(stats)
    if stats:
        return 'skip_upload_dummy'
    else:
        return 'upload_folder_to_gcs'

def upload_folder_to_gcs(**kwargs):
    data_folder = config["local_dataset_download_path"]
    bucket_name = config["linkedin_data_bucket_name"]
    gcs_conn_id = config["airflow_gcp_conn_id"]

    # Uploading all the csv files except for the job_summary.csv
    csv_files = [file for file in os.listdir(data_folder) if file.endswith('.csv') and file != 'job_summary.csv']

    for csv_file in csv_files:
        local_file_path = os.path.join(data_folder, csv_file)
        gcs_file_path = f"downloaded_data/{csv_file}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id='uploading_to_gcs',
            src=local_file_path,
            dst=gcs_file_path,
            bucket=bucket_name,
            gcp_conn_id=gcs_conn_id,
            chunk_size=5*1024*1024
        )
        upload_task.execute(context=kwargs)
