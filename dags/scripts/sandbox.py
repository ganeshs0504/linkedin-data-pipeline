import os
import zipfile
import subprocess
import logging
import pandas as pd

from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
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
            subprocess.run(['kaggle', 'datasets', 'download', 'satvshr/top-4-used-car-sales-datasets-combined'], check=True)
        except Exception as e:
            logging.error(f"Failed to download dataset: {e}")
        with zipfile.ZipFile('top-4-used-car-sales-datasets-combined.zip', 'r') as zip_ref:
            zip_ref.extractall(downloaded_data_path)


def print_loaded_csv_callable():
    downloaded_data_path = 'raw_data/downloaded_data/output.csv'
    temp = pd.read_csv(downloaded_data_path)
    print(temp.head(5))


def check_if_file_exists_in_gcs():
    name = 'downloaded_data/output.csv'
    storage_client = storage.Client.from_service_account_json('/keys/gcp_creds.json')
    bucket_name = 'test_bucket_airflow_99'
    bucket = storage_client.bucket(bucket_name)
    stats = storage.Blob(bucket=bucket, name=name).exists(storage_client)
    logging.info("~~~~~~FILE EXISTS?~~~~~~~~~~~~~~~~~~~~~~~~")
    logging.info(stats)
    if stats:
        return 'bash_placeholder'
    else:
        return 'upload_file_to_gcs'


