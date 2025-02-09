import json
import logging
from pathlib import Path
from typing import Union

import yaml
from pydantic import BaseModel, ValidationError, Field

from pydantic import BaseModel, Field, model_validator
from typing import List, Optional


class Default(BaseModel):
    bucket: str
    expected_files: List[str]


class FHIRConfig(BaseModel):
    project_id: str
    location_id: str
    fhir_store_id: str


class Project(BaseModel):
    id: str
    bucket: Optional[str] = None
    expected_files: Optional[List[str]] = None


class Config(BaseModel):
    defaults: Default
    projects: List[Project]
    fhir: FHIRConfig

    @model_validator(mode="after")
    def set_defaults(self, values):
        if not self.projects or len(self.projects) == 0:
            raise ValidationError("Expected at least one project")
        if not self.defaults:
            raise ValidationError("Expected default config")
        if not self.fhir:
            raise ValidationError("Expected fhir config")
        for p in self.projects:
            if not p.bucket:
                p.bucket = self.defaults.bucket
            if not p.expected_files:
                p.expected_files = self.defaults.expected_files

        return self



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
