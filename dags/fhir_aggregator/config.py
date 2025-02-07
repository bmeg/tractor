import json
import logging
from pathlib import Path
from typing import Union

import yaml
from pydantic import BaseModel, ValidationError, Field


class Config(BaseModel):
    """Configuration parameters for the data pipeline."""

    prefix: str = Field(..., description="Prefix for DAG and dataset IDs.")
    bucket: str = Field(
        ...,
        description="Name of the Google Cloud Storage bucket containing input data.",
    )
    expected_files: list[str] = Field(
        ..., description="List of expected file names in the input bucket."
    )
    output_bucket: str = Field(
        ...,
        description="Name of the Google Cloud Storage bucket for storing transformed data.",
    )
    project_id: str = Field(..., description="Your Google Cloud Project ID.")
    location_id: str = Field(
        ...,
        description="The location of your Google Cloud project (e.g., 'us-central1').",
    )
    fhir_store_id: str = Field(
        ..., description="The ID of your Google Healthcare FHIR store."
    )


def load_config(config_path: Union[str, Path]) -> Config:
    """Loads configuration from a JSON or YAML file.

    Args:
        config_path: Path to the configuration file (JSON or YAML).

    Returns:
        A Config object containing the loaded configuration.

    Raises:
        FileNotFoundError: If the config file is not found.
        ValidationError: If the config file is invalid.
        ValueError: If the config file is neither JSON nor YAML.

    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    try:
        with open(config_path, "r") as f:
            if config_path.suffix == ".json":
                config_dict = json.load(f)
            elif config_path.suffix in (".yaml", ".yml"):
                config_dict = yaml.safe_load(f)
            else:
                raise ValueError("Unsupported config file format. Use JSON or YAML.")
            config = Config(**config_dict)
            return config
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        raise ValidationError(f"Invalid config file format: {e}")


def load_default_config():

    config_path = "/opt/airflow/projects/fhir-aggregator/config.yaml"
    if not config_path:
        raise ValueError("config_path must be specified in dag_run.conf")

    try:
        return load_config(config_path)
    except Exception as e:
        logging.warning(f"Error loading config from {config_path}: {e}")
        raise e
