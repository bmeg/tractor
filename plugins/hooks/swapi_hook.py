import json
import logging
from typing import Any

from json2html import *
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from pydantic import HttpUrl, BaseModel
from sensors.etag_change_sensor import check_etag

# application code


class SwapiConfig(BaseModel):
    """ Configuration for the Star Wars API, uses Pydantic for validation """
    url: HttpUrl
    """URL to download the catalog from"""
    bucket_name: str
    """Name of the S3 bucket to store the catalog"""
    s3_prefix: str
    """Prefix for the S3 key for catalog files"""
    aws_conn_id: str
    """Airflow connection ID for the AWS credentials"""
    etag_variable_name: str
    """Name of the Airflow Variable to store the ETag"""


class SwapiHook(BaseHook):
    """ Hook to interact with the Star Wars API, base class for ETL tasks """
    def __init__(self, swapi_config_dict: dict[str, Any]):
        # require a config
        if not swapi_config_dict:
            raise ValueError("swapi_config is required")
        # parse the config
        self.config = SwapiConfig(**swapi_config_dict)
        # create the S3 hook
        self.s3_hook = S3Hook(aws_conn_id=self.config.aws_conn_id)

    def download_to_s3(self, url: str, file_name: str, confirm_upload: bool = True) -> requests.Response:
        """ Download a file from a URL and upload it to S3
        :param url: URL to download the file from
        :param file_name: Name of the file to save in S3
        :param confirm_upload: Check if the file was uploaded successfully
        """
        response = requests.get(url)
        s3_key = f"{self.config.s3_prefix}/{file_name}"
        if response.status_code == 200:
            logging.info(f"Downloaded from {self.config.url}")
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

    def run(self) -> bool:
        """ Run the ETL task """
        raise NotImplementedError("Subclasses must implement the run method")


class CheckETag(SwapiHook):
    """ Task to check if the ETag of the Star Wars API has changed """
    def run(self) -> bool:
        """ Check if the ETag of the Star Wars API has changed """
        return check_etag(url=self.config.url, variable_name=self.config.etag_variable_name)


class Extract(SwapiHook):
    """ Task to download the Star Wars API catalog to S3
    See https://swapi.dev/documentation#root
    """
    def run(self) -> bool:
        """ Download the Star Wars API catalog and resources to S3 """
        response = self.download_to_s3(self.config.url, "swapi.json")
        root = response.json()
        for resource_name, url in root.items():
            self.download_to_s3(url=url, file_name=f"{resource_name}.json")
        return True


class Transform(SwapiHook):
    """ Read extracted data from S3 and transform it """
    def run(self) -> bool:
        """ Transform the Star Wars API resources from JSON to HTML """
        keys = self.s3_hook.list_keys(bucket_name=self.config.bucket_name, prefix=self.config.s3_prefix)
        for key in keys:
            if key.endswith(".json"):
                resource = json.loads(self.s3_hook.read_key(key=key, bucket_name=self.config.bucket_name))
                # Transform the resource
                html = json2html.convert(json=resource)
                # Save the transformed resource
                self.s3_hook.load_string(
                    html,
                    key=key.replace(".json", ".html"),
                    bucket_name=self.config.bucket_name,
                    replace=True
                )
        return True


class Load(SwapiHook):
    """ Task to load the Star Wars html into a webserver """
    def run(self) -> bool:
        """ Load the Star Wars API resources into a webserver """
        keys = self.s3_hook.list_keys(bucket_name=self.config.bucket_name, prefix=self.config.s3_prefix)
        for key in keys:
            if key.endswith(".html"):
                logging.info(f"Loading {key} into the webserver")
        return True
