import json

import yaml
from jsonschema import validate
import pytest


# Load the schema
@pytest.fixture
def schema():
    # with open('dags/tractor/etl_dsl_schema.json', 'r') as schema_file:
    #     schema = json.load(schema_file)
    #     return schema
    with open("dags/tractor/etl_dsl/etl_dsl_schema.yaml", "r") as schema_file:
        schema = yaml.safe_load(schema_file)
        return schema


@pytest.fixture
def minimal_etl_project():
    with open("tests/fixtures/etl-dsl/minimal.yaml", "r") as file:
        return yaml.safe_load(file)


@pytest.fixture
def multi_source_etl_project():
    with open("tests/fixtures/etl-dsl/multi-source.yaml", "r") as file:
        return yaml.safe_load(file)
