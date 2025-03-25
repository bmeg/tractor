import jsonschema
from unittest.mock import mock_open, patch
import pytest
import json


# Define a function to validate the schema
def test_validate_schema(schema):
    jsonschema.Draft7Validator.check_schema(schema)


# Test for invalid JSON error
def test_invalid_json_error():
    invalid_json_content = '{"key": "value"'
    with patch("builtins.open", mock_open(read_data=invalid_json_content)):
        with pytest.raises(json.JSONDecodeError):
            with open('dags/tractor/etl_dsl/etl_dsl_schema.json', 'r') as json_file:
                json.load(json_file)
