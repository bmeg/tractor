import logging
import os
from datetime import timedelta, datetime
from typing import Any

import yaml
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.providers.http.sensors.http import HttpSensor
from pydantic import BaseModel, Extra, model_validator, ValidationError

from tractor.extractors import download_to_s3
from tractor.http_response_checkers import check_md5, check_etag, check_last_modified


class ResponseCheck(BaseModel):
    """
    Model for response check configuration.

    Attributes:
        relative_url (str): Relative URL to check.
        type (str): Type of check to perform (default is "md5").
        checker (callable): Function to use for checking the response.
        method (str): HTTP method to use for the check.
        poke_interval (int): Interval in seconds to wait between checks (default is 30).
        timeout (int): Timeout in seconds for the check (default is 120).
    """

    relative_url: str
    type: str = "md5"
    checker: callable = None
    method: str = None
    poke_interval: int = 30
    timeout: int = 59


class Config(BaseModel):
    """
    Model for DAG configuration.

    Attributes:
        dag_id (str): ID of the DAG.
        response_check (ResponseCheck): Configuration for response check.
        tags (list[str]): List of tags for the DAG.
        aws_conn_id (str): Airflow connection ID for AWS.
        bucket_name (str): Name of the S3 bucket.
        s3_prefix (str): Prefix for the S3 key.
        schedule_dataset (str | Dataset): Dataset to schedule the DAG.
        raw_dataset (str | Dataset): Raw dataset for the DAG.
        http_conn_id (str): Airflow connection ID for HTTP.
        inputs (dict[str, str]): Dictionary of input sources.
    """

    dag_id: str
    description: str = "Dynamic ETL DAGs"
    start_date: datetime = None
    response_check: ResponseCheck = None
    tags: list[str] = []
    aws_conn_id: str = None
    bucket_name: str = None
    s3_prefix: str = None
    schedule_dataset: str | Dataset = None
    raw_dataset: str | Dataset = None
    processed_dataset: str | Dataset = None
    http_conn_id: str = None
    inputs: dict[str, str] = {}

    class Config:
        extra = Extra.allow
        arbitrary_types_allowed = True

    @staticmethod
    def days_ago(n):
        """Return a date that is `n` days ago."""
        return datetime.now() - timedelta(days=n)

    @model_validator(mode="after")
    def check_config(self):
        if not self.dag_id:
            raise ValidationError("DAG ID must be provided in the config.")
        if not self.inputs:
            raise ValidationError("At least one input must be provided in the config.")
        if not self.aws_conn_id:
            self.aws_conn_id = f"{self.dag_id}-aws"
        if not self.bucket_name:
            self.bucket_name = "EllrottLab"
        if not self.s3_prefix:
            self.s3_prefix = f"tractor/{self.dag_id}"
        if not self.http_conn_id:
            self.http_conn_id = f"{self.dag_id}-http"
        if self.schedule_dataset and isinstance(self.schedule_dataset, str):
            self.schedule_dataset = Dataset(self.schedule_dataset)
        if self.raw_dataset and isinstance(self.raw_dataset, str):
            self.raw_dataset = Dataset(self.raw_dataset)
        else:
            self.raw_dataset = Dataset(f"{self.dag_id}_raw")
        if self.processed_dataset and isinstance(self.processed_dataset, str):
            self.processed_dataset = Dataset(self.processed_dataset)
        else:
            self.processed_dataset = Dataset(f"{self.dag_id}_processed")

        if not self.response_check:
            self.response_check = ResponseCheck(
                relative_url=next(iter(self.inputs.values()))
            )
        if not self.response_check.checker:
            self.response_check.checker = {
                "md5": check_md5,
                "etag": check_etag,
                "last_modified": check_last_modified,
            }.get(self.response_check.type, None)
            if self.response_check.checker is None:
                raise ValidationError(
                    f"Invalid response check type: {self.response_check.type}"
                )

            self.response_check.method = (
                "GET" if self.response_check.type == "md5" else "HEAD"
            )

        self.tags = self.tags + [self.dag_id, "dynamic"]
        self.description = (
            f"{self.description} (Generated: {datetime.now().isoformat()})"
        )
        if not self.start_date:
            self.start_date = self.days_ago(1)
        return self




def read_config(config_path: str):
    """Reads a YAML configuration file."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def generate_extract_dag(config: Config) -> DAG:
    """Generate a DAG for extracting data from a source."""
    base_dag_id = config.dag_id
    # add suffix to the dag_id for each generated dag
    dag_id = f"{base_dag_id}_extract"

    schedule_dataset = config.schedule_dataset or None

    dag_properties = {
        "dag_id": dag_id,
        "start_date": config.start_date,
        "catchup": False,
        "tags": config.tags + ["extract"],
        "description": f"{config.description} (extract)",
        "is_paused_upon_creation": False,
    }
    if schedule_dataset:
        dag_properties["schedule"] = [schedule_dataset]
    else:
        dag_properties["schedule_interval"] = "@daily"

    with DAG(**dag_properties) as dag:
        # Check Task

        check_task = HttpSensor(
            task_id="check",
            http_conn_id=config.http_conn_id,  # Ensure Airflow has this connection configured
            endpoint=config.response_check.relative_url,
            method=config.response_check.method,
            response_check=config.response_check.checker,
            poke_interval=config.response_check.poke_interval,  # Check every N seconds
            timeout=config.response_check.timeout,  # Fail after N seconds
            soft_fail=True,  # a timeout is not a failure, just means there is no data
        )

        # Extract Task
        @task(outlets=[config.raw_dataset])
        def extract_task(outlet_events) -> list[str]:
            bucket_urls = []
            for name, relative_url in config.inputs.items():
                response, bucket_url = download_to_s3(
                    http_conn_id=config.http_conn_id,
                    relative_url=relative_url,
                    aws_conn_id=config.aws_conn_id,
                    bucket_name=config.bucket_name,
                    s3_prefix=config.s3_prefix,
                    file_name=name,
                )
                bucket_urls.append(bucket_url)
            # write the manifest to the dataset's metadata
            outlet_events[config.raw_dataset].extra = {"manifest": bucket_urls}
            return bucket_urls

        # Define Dependencies
        # this has been problematic, so we are using the following workaround
        # pylint: disable=pointless-statement
        # https://github.com/pylint-dev/pylint/issues/2584
        # https://github.com/apache/airflow/discussions/27412#discussioncomment-4028635
        [check_task >> extract_task()]
        return dag


def generate_transform_dag(config: Config):
    """Generate a DAG for transforming data."""
    base_dag_id = config.dag_id
    dag_id = f"{base_dag_id}_transform"

    dag_properties = {
        "dag_id": dag_id,
        "start_date": config.start_date,
        "catchup": False,
        "tags": config.tags + ["transform"],
        "schedule": [config.raw_dataset],
        "description": f"{config.description} (transform)",
    }

    with DAG(**dag_properties) as dag:

        @task(inlets=[config.raw_dataset])
        def transform_task(inlet_events, *args, **kwargs) -> Any:
            """Transform the extracted data, store results in S3"""
            # get the manifest from the event history for the raw_dataset
            events = inlet_events[config.raw_dataset]
            last_event = events[-1]
            assert last_event, f"No events found for {config.raw_dataset}"
            assert "manifest" in last_event.extra, f"No manifest found in {last_event}"
            manifest = last_event.extra["manifest"]
            logging.info(f"Extracted data: {manifest}")
            # TODO Implement data transformation
            manifest = [_.replace(".yaml", ".html") for _ in manifest]
            manifest = [_.replace(".json", ".html") for _ in manifest]
            return manifest

        @task(outlets=[config.processed_dataset])
        def validate_task(transform_result: Any, outlet_events) -> Any:
            """QA the Transformation, validate the data, return the transformed data"""
            logging.info(f"Transformed data: {transform_result}")
            for _ in transform_result:
                assert _.endswith(
                    ".html"
                ), f"Invalid file extension: {transform_result}"
            validated_results = [_ for _ in transform_result]
            outlet_events[config.processed_dataset].extra = {
                "manifest": validated_results
            }

            return validated_results

        # Define Dependencies
        # TODO TypeError: unsupported operand type(s) for >>: '_TaskDecorator' and '_TaskDecorator'
        # transform_task >> validate_task
        # TODO AttributeError: '_TaskDecorator' object has no attribute 'set_downstream'
        # transform_task.set_downstream(validate_task)
        validate_task(transform_task())
        return dag


def generate_load_dag(config: Config):
    """Generate a DAG for loading data."""
    base_dag_id = config.dag_id
    dag_id = f"{base_dag_id}_load"

    dag_properties = {
        "dag_id": dag_id,
        "start_date": config.start_date,
        "catchup": False,
        "tags": config.tags + ["load"],
        "schedule": [config.processed_dataset],
        "description": f"{config.description} (load)",
    }

    with DAG(**dag_properties) as dag:

        @task(inlets=[config.processed_dataset])
        def load_task(inlet_events, *args, **kwargs) -> Any:
            """Load the validated data, store results in some data sink"""
            # get the manifest from the event history for the processed_dataset
            events = inlet_events[config.processed_dataset]
            last_event = events[-1]
            assert last_event, f"No events found for {config.processed_dataset}"
            assert "manifest" in last_event.extra, f"No manifest found in {last_event}"
            manifest = last_event.extra["manifest"]
            logging.info(f"Extracted data: {manifest}")
            # TODO Implement load
            manifest = [f"Loaded {_}" for _ in manifest]
            return manifest

        # Define Dependencies
        load_task()

        return dag


def generate(config_file_path: str = None) -> list[DAG]:
    """Generates a set of ETL DAGs from yaml files"""
    if os.path.exists(config_file_path):
        config = Config(**read_config(config_file_path))
        return [
            generate_extract_dag(config),
            generate_transform_dag(config),
            generate_load_dag(config),
        ]
    else:
        raise FileNotFoundError(f"Config dir not found: {config_file_path}")
