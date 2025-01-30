from airflow import DAG
# from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime
import logging

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}


from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import json

# # Configuration
# {"url": "https://swapi.dev/api/", "bucket_name": "EllrottLab", "s3_prefix": "tractor/swapi/", "aws_conn_id": "ceph2"}
# Function to download and upload to S3


def swapi_catalog():

    swapi_config = Variable.get("swapi_config", default_var=None)
    assert swapi_config, f"Could not read swapi_config variable"
    swapi_config = json.loads(swapi_config)
    expected_keys = sorted(["url", "bucket_name", "s3_prefix", "aws_conn_id"])
    assert sorted(swapi_config.keys()) == expected_keys, "swapi_config missing expected keys"

    s3_hook = S3Hook(aws_conn_id=swapi_config["aws_conn_id"])

    response = requests.get(swapi_config["url"])
    s3_key = f"{swapi_config["url"]}/swapi.json"
    if response.status_code == 200:
        logging.info(f"Downloaded from {swapi_config["url"]}")
        s3_hook.load_bytes(
            response.content,
            key=s3_key,
            bucket_name=swapi_config["bucket_name"],
            replace=True
        )
        logging.info(f"File uploaded to s3://{swapi_config["bucket_name"]}/{s3_key}")
        exists = s3_hook.check_for_key(key=s3_key,bucket_name=swapi_config["bucket_name"])
        logging.info(f"s3://{swapi_config["bucket_name"]}/{s3_key} exists? {exists}")
    else:
        raise Exception(f"Failed to download file, status code: {response.status_code}")

# Define the DAG
default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}
dag = DAG("swapi_catalog", default_args=default_args, schedule_interval=None)

# Airflow Task
download_task = PythonOperator(
    task_id="swapi_catalog",
    python_callable=swapi_catalog,
    dag=dag
)
