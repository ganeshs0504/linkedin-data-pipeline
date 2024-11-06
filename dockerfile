FROM apache/airflow:2.10.2
COPY requirements.txt /requirements.txt
COPY keys/gcs-linkedin-pipeline-key.json /keys/gcp_creds.json
COPY config.json ./config.json
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt