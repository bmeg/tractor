import logging

import requests
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook


def download_to_s3(http_conn_id: str, relative_url: str,
                   aws_conn_id: str | None, bucket_name: str, s3_prefix: str, file_name: str,
                   confirm_s3_upload: bool = False) -> (requests.Response, str):
    """ Download a file from a URL and upload it to S3
    :param http_conn_id: Airflow connection ID for the HTTP connection
    :param relative_url: relative URL to download the file from
    :param aws_conn_id: Airflow connection ID for the AWS connection, None for default
    :param bucket_name: Name of the S3 bucket to upload the file to
    :param s3_prefix: Prefix for the S3 key
    :param file_name: Name of the file to save in S3
    :param confirm_s3_upload: Check if the file was uploaded successfully
    returns a tuple of the response and the bucket_url
    """

    logging.info(f"Downloading from http_conn_id:{http_conn_id} {relative_url} to s3://{bucket_name}/{s3_prefix}/{file_name} using {aws_conn_id}")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    http_hook = HttpHook(method="GET", http_conn_id=http_conn_id)
    response = http_hook.run(endpoint=relative_url)

    s3_key = f"{s3_prefix}/{file_name}"
    if response.status_code == 200:
        bucket_url = f"s3://{bucket_name}/{s3_key}"
        logging.info(f"Downloaded from {relative_url}, saving to {bucket_url}")
        s3_hook.load_bytes(
            response.content,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        logging.info(f"File uploaded to {bucket_url}")
        if confirm_s3_upload:
            exists = s3_hook.check_for_key(key=s3_key, bucket_name=bucket_name)
            logging.info(f"s3://{bucket_name}/{s3_key} exists? {exists}")
            if not exists:
                raise Exception(f"Failed to upload file to s3://{bucket_name}/{s3_key}")
        return response, bucket_url
    else:
        raise Exception(f"Failed to download file, status code: {response.status_code}")
