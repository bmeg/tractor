import logging
from abc import abstractmethod
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, AnyUrl, Extra
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
import requests


class BaseETLConfig(BaseModel):
    """Base class for ETL configuration, uses Pydantic for validation"""

    dag_id: str
    """DAG ID for the ETL task"""
    inlet_datasets: list[str] = []
    """
    List of dataset names, updated by upstream producers, that triggered this workflow.
    See https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html
    """
    outlet_datasets: list[str] = []
    """List of dataset names, produced by this workflow, that may trigger downstream workflows."""

    inputs: dict[str, AnyUrl] = {}
    """Dictionary of named input source urls (S3 paths, file paths, urls, etc)"""
    outputs: dict[str, AnyUrl] = {}
    """
    Dictionary of named output source url (S3 paths, file paths, urls, etc)
    TODO: deprecate? use outlet_datasets instead?
    """

    aws_conn_id: str = "aws_default"
    """Airflow connection ID for the AWS credentials"""
    bucket_name: str = None
    """Name of the S3 bucket to store the catalog"""
    s3_prefix: str = None
    """Prefix for the S3 key for catalog files"""

    sensor_variable_name: str = None
    """Name of the Airflow Variable to store the sensor state e.g. ETag, Last-Modified"""

    class Config:
        extra = Extra.allow


class BaseETLHook(BaseHook):
    """Base class for ETL Hooks"""

    def __init__(self, config: dict[str, Any] | BaseModel, **kwargs):
        """Initialize the ETL Hook with a configuration, set up the S3 hook"""
        super().__init__(**kwargs)
        # require a config
        if not config:
            raise ValueError("config is required")
        # parse the config, if necessary
        if isinstance(config, dict):
            self.config = BaseETLConfig(**config)
        else:
            self.config = config

        # placeholder for the S3 hook
        self._s3_hook = None
        logging.info(f"Initialized {type(self).__name__}")

    @property
    def s3_hook(self):
        """Lazy load the S3 hook"""
        if not self._s3_hook:
            self._s3_hook = S3Hook(aws_conn_id=self.config.aws_conn_id)
        return self._s3_hook

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """Perform task-specific ETL operations."""
        pass

    # common helper methods

    @staticmethod
    def parse_http_hook_url(url: str | AnyUrl) -> (str, str):
        """Parse the `http_hook://` URL to extract the connection_id and relative path.
        returns (http_conn_id, relative_url)
        """
        if isinstance(url, AnyUrl):
            http_conn_id = url.host
            relative_url = url.path
        elif url.startswith("http-hook://"):
            parsed = urlparse(url)
            http_conn_id = parsed.netloc
            relative_url = parsed.path
        else:
            raise ValueError(f"Invalid URL: {url}, must start with http-hook://")
        return http_conn_id, relative_url

    def download_to_s3(self, http_conn_id: str, url: str, file_name: str, confirm_upload: bool = False) -> (requests.Response, str):
        """ Download a file from a URL and upload it to S3
        :param http_conn_id: Airflow connection ID for the HTTP connection
        :param url: relative URL to download the file from
        :param file_name: Name of the file to save in S3
        :param confirm_upload: Check if the file was uploaded successfully
        returns a tuple of the response and the bucket_url
        """

        http_hook = HttpHook(method="GET", http_conn_id=http_conn_id)
        response = http_hook.run(endpoint=url)
        s3_key = f"{self.config.s3_prefix}/{file_name}"
        if response.status_code == 200:
            bucket_url = f"s3://{self.config.bucket_name}/{s3_key}"
            logging.info(f"Downloaded from {url}, saving to {bucket_url}")
            self.s3_hook.load_bytes(
                response.content,
                key=s3_key,
                bucket_name=self.config.bucket_name,
                replace=True
            )
            logging.info(f"File uploaded to {bucket_url}")
            if confirm_upload:
                exists = self.s3_hook.check_for_key(key=s3_key, bucket_name=self.config.bucket_name)
                logging.info(f"s3://{self.config.bucket_name}/{s3_key} exists? {exists}")
                if not exists:
                    raise Exception(f"Failed to upload file to s3://{self.config.bucket_name}/{s3_key}")
            return response, bucket_url
        else:
            raise Exception(f"Failed to download file, status code: {response.status_code}")

    def validate(self, *args, **kwargs) -> bool:
        """Validate the ETL task"""
        # TODO - add common validation logic for self.inputs
        return True
