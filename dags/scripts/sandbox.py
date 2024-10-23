import os
import zipfile
import subprocess
import logging
import pandas as pd

log = logging.getLogger("airflow")
log.setLevel(logging.INFO)
log.info("Yes, you will see this log :)")

from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
import json


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


