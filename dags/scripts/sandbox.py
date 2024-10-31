import os
import zipfile
import subprocess
import logging
from glob import glob

from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import json

from google.cloud import storage


def download_and_extract_raw_data_callable():
    kaggle_conn = BaseHook.get_connection('kaggle_api')
    kaggle_creds = json.loads(kaggle_conn.extra)
    os.environ['KAGGLE_USERNAME'] = kaggle_creds['username']
    os.environ['KAGGLE_KEY'] = kaggle_creds['key']

    downloaded_data_path = 'raw_data/downloaded_data'

    if os.path.exists(downloaded_data_path):
        logging.warning("FILE ALREADY DOWNLOADED")
        pass
    else:
        try:
            subprocess.run(['kaggle', 'datasets', 'download', 'asaniczka/1-3m-linkedin-jobs-and-skills-2024'], check=True)
        except Exception as e:
            logging.error(f"Failed to download dataset: {e}")
        with zipfile.ZipFile('1-3m-linkedin-jobs-and-skills-2024.zip', 'r') as zip_ref:
            zip_ref.extractall(downloaded_data_path)

def check_if_file_exists_in_gcs():
    name = 'downloaded_data/job_skills.csv'
    storage_client = storage.Client.from_service_account_json('/keys/gcp_creds.json')
    bucket_name = 'gcs-linkedin-data-bucket'
    bucket = storage_client.bucket(bucket_name)
    stats = storage.Blob(bucket=bucket, name=name).exists(storage_client)
    logging.info("~~~~~~FILE EXISTS?~~~~~~~~~~~~~~~~~~~~~~~~")
    logging.info(stats)
    if stats:
        return 'skip_upload_dummy'
    else:
        return 'upload_folder_to_gcs'


# def upload_folder_to_gcs():
#     connection = BaseHook.get_connection("gcp_creds")
#     # client = storage.Client.from_service_account_json(connection.extra_dejson.get('extra__google_cloud_platform__keyfile_dict'))
#     client = storage.Client.from_service_account_json('/keys/gcp_creds.json')
#     bucket = client.bucket("gcs-linkedin-data-bucket")
#     source_folder = "raw_data/downloaded_data"

#     for root, _, files in os.walk(source_folder):
#         for file in files:
#             local_file_path = os.path.join(root, file)
#             rel_path = os.path.relpath(local_file_path, source_folder)
#             dest_blob_name = os.path.join("downloaded_data", rel_path)

#             blob = bucket.blob(dest_blob_name)
#             blob.upload_from_filename(local_file_path)


def upload_folder_to_gcs(**kwargs):
    data_folder = "raw_data/downloaded_data"
    bucket_name = "gcs-linkedin-data-bucket"
    gcs_conn_id = "gcp_creds"

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
