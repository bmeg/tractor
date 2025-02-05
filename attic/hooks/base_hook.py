import logging
from abc import abstractmethod
from typing import Any

from pydantic import BaseModel, AnyUrl, Extra
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
import requests


class BaseETLConfig(BaseModel):
    """Base class for ETL configuration, uses Pydantic for validation"""

    inputs: dict[str, AnyUrl] = {}
    """Dictionary of named input source urls (S3 paths, file paths, urls, etc)"""
    outputs: dict[str, AnyUrl] = {}
    """Dictionary of named input source url (S3 paths, file paths, urls, etc)"""
    aws_conn_id: str = "aws_default"
    """Airflow connection ID for the AWS credentials"""
    bucket_name: str = None
    """Name of the S3 bucket to store the catalog"""
    s3_prefix: str = None
    """Prefix for the S3 key for catalog files"""
    sensor_variable_name: str = None
    """Name of the Airflow Variable to store the sensor state e.g. ETag"""

    class Config:
        extra = Extra.allow


class BaseETLHook(BaseHook):
    """Base class for ETL Hooks"""

    def __init__(self, config: dict[str, Any], inputs: Any | None = None, **kwargs):
        """Initialize the ETL Hook with a configuration, set up the S3 hook"""
        # require a config
        if not config:
            raise ValueError("config is required")
        # parse the config
        self.config = BaseETLConfig(**config)
        # create the S3 hook
        self.s3_hook = S3Hook(aws_conn_id=self.config.aws_conn_id)
        # set the inputs
        self.inputs = inputs
        logging.info(f"Initialized {type(self).__name__} with inputs: {self.inputs}")

    def download_to_s3(self, url: str, file_name: str, confirm_upload: bool = True) -> requests.Response:
        """ Download a file from a URL and upload it to S3
        :param url: URL to download the file from
        :param file_name: Name of the file to save in S3
        :param confirm_upload: Check if the file was uploaded successfully
        """
        response = requests.get(url)
        s3_key = f"{self.config.s3_prefix}/{file_name}"
        if response.status_code == 200:
            logging.info(f"Downloaded from {url}, saving to s3://{self.config.bucket_name}/{s3_key}")
            self.s3_hook.load_bytes(
                response.content,
                key=s3_key,
                bucket_name=self.config.bucket_name,
                replace=True
            )
            logging.info(f"File uploaded to s3://{self.config.bucket_name}/{s3_key}")
            if confirm_upload:
                exists = self.s3_hook.check_for_key(key=s3_key, bucket_name=self.config.bucket_name)
                logging.info(f"s3://{self.config.bucket_name}/{s3_key} exists? {exists}")
                if not exists:
                    raise Exception(f"Failed to upload file to s3://{self.config.bucket_name}/{s3_key}")
            return response
        else:
            raise Exception(f"Failed to download file, status code: {response.status_code}")

    def validate(self) -> bool:
        """Validate the ETL task"""
        logging.info(f"TODO - add common validation logic for {type(self.inputs)}")
        return True

    @abstractmethod
    def run(self) -> bool:
        """Run the ETL task"""
        raise NotImplementedError("Subclasses must implement the run method")
