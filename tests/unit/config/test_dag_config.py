import os
import sys

import pytest
import tes
import yaml
from airflow import Dataset
from attrs import asdict
from tractor.config import DagConfig, Default, Project


@pytest.fixture
def defaults():
    return Default(**{"bucket": "bucket1", "bucket_scheme": "s3"})


def test_should_ensure_inputs(defaults):
    """Ensure that inputs are created correctly."""
    user_supplied_inputs = ["input/file1.txt", "input/file2.txt", "input/dir1"]
    config = DagConfig(id="my-id", name="My Name", inputs=user_supplied_inputs, defaults=defaults)
    assert all([type(_) is tes.Input for _ in config.inputs]), f"Expected all inputs to be of type Input, got {config.inputs}"
    inputs: list[tes.Input] = config.inputs
    assert len(inputs) == 3, f"Expected 3 inputs, got {len(inputs)}"
    for i, _input in enumerate(inputs):
        assert _input.name == user_supplied_inputs[i].split("/")[-1], f"Expected input name to be {user_supplied_inputs[i]}, got {_input.name}"
        assert _input.type == "FILE" if "." in user_supplied_inputs[i] else "DIRECTORY", f"Expected input type to be FILE or DIRECTORY, got {_input.type}"
        assert _input.url == f"s3://bucket1/{user_supplied_inputs[i]}", f"Expected input url to be s3://bucket1/{user_supplied_inputs[i]}, got {_input.url}"
        assert _input.path == user_supplied_inputs[i], f"Expected input path to be {user_supplied_inputs[i]}, got {_input.path}"
        assert _input.description == "Generated from config file", f"Expected input description to be 'Generated from config file', got {_input.description}"

    print(f"OK: Created config with inputs: {config.inputs} based on {user_supplied_inputs}")


def test_should_ensure_outputs(defaults):
    """Ensure that inputs are created correctly."""
    user_supplied_outputs = ["output/file1.txt", "output/file2.txt", "output/dir1"]
    config = DagConfig(id="my-id", name="My Name", outputs=user_supplied_outputs, defaults=defaults, inputs=["foo"])
    assert all([type(_) is tes.Output for _ in config.outputs]), f"Expected all outputs to be of type Output, got {config.outputs}"
    for i, output in enumerate(config.outputs):
        assert output.name == user_supplied_outputs[i].split("/")[-1], f"Expected input name to be {user_supplied_outputs[i]}, got {output.name}"
        assert output.type == "FILE" if "." in user_supplied_outputs[i] else "DIRECTORY", f"Expected input type to be FILE or DIRECTORY, got {output.type}"
        assert output.url == f"s3://bucket1/{user_supplied_outputs[i]}", f"Expected input url to be s3://bucket1/{user_supplied_outputs[i]}, got {output.url}"
        assert output.path == user_supplied_outputs[i], f"Expected input path to be {user_supplied_outputs[i]}, got {output.path}"
        assert output.description == "Generated from config file", f"Expected input description to be 'Generated from config file', got {output.description}"


def test_should_validate_id_and_name():
    """Ensure that id and name are required."""
    with pytest.raises(TypeError):
        _ = DagConfig()


def test_should_ensure_schedule(defaults):
    """Ensure that the schedule is set."""
    config = DagConfig(id="my-id", name="My Name", schedule=["fhir"], inputs=["input/file1.txt", "input/file2.txt", "input/dir1"], outputs=["output/file1.txt", "output/file2.txt", "output/dir1"], defaults=defaults)
    assert all([type(_) is Dataset for _ in config.schedule]), f"Expected schedule to be list[Dataset], got {config.schedule}"
    config = DagConfig(id="my-id", name="My Name", inputs=["input/file1.txt", "input/file2.txt", "input/dir1"], outputs=["output/file1.txt", "output/file2.txt", "output/dir1"], defaults=defaults)
    assert not config.schedule, f"Expected schedule to be null, got {config.schedule}"
    config = DagConfig(id="my-id", name="My Name", schedule="foo", inputs=["input/file1.txt", "input/file2.txt", "input/dir1"], outputs=["output/file1.txt", "output/file2.txt", "output/dir1"], defaults=defaults)
    assert config.schedule == "foo", f"Expected schedule to be `foo`, got {config.schedule}"


def test_to_yaml(defaults):
    """Ensure that the config can be serialized."""
    config = DagConfig(id="my-id", name="My Name", schedule=["fhir"], inputs=["input/file1.txt", "input/file2.txt", "input/dir1"], outputs=["output/file1.txt", "output/file2.txt", "output/dir1"], defaults=defaults)
    config_dict = asdict(config)
    assert isinstance(config_dict, dict), "Expected config to be serializable"
    yaml.dump(config_dict, default_flow_style=False, sort_keys=False, stream=sys.stdout)
    print(f"OK: Serialized config: {config_dict}")


@pytest.fixture
def invalid_config():
    with open(os.path.join('tests', 'fixtures', 'configs', 'invalid.yaml'), 'r') as file:
        return yaml.load(file.read(), Loader=yaml.FullLoader)


@pytest.fixture
def valid_config():
    with open(os.path.join('tests', 'fixtures', 'configs', 'valid.yaml'), 'r') as file:
        return yaml.load(file.read(), Loader=yaml.FullLoader)


@pytest.fixture
def mixed_operators_config():
    with open(os.path.join('tests', 'fixtures', 'configs', 'mixed-operators.yaml'), 'r') as file:
        return yaml.load(file.read(), Loader=yaml.FullLoader)


def test_should_error_on_bad_config(invalid_config):
    """Ensure that the config can be deserialized."""
    with pytest.raises(TypeError):
        _ = DagConfig(**invalid_config)


def test_should_load_valid_config(valid_config, defaults):
    """Ensure that the config can be deserialized."""
    _ = DagConfig(**valid_config, defaults=defaults)


def test_should_load_mixed_operators_config(mixed_operators_config):
    """Ensure that the config can be deserialized."""
    project = Project(**mixed_operators_config)
    assert project, "Expected project to be created"
    print(f"OK: Created project: {project}")
    assert project.sources, "Expected sources to be created"
    assert all([isinstance(_, DagConfig) for _ in project.sources]), f"Expected sources to be of type DagConfig. Got {project.sources[0]}"



