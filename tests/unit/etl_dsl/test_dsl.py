import jsonschema
import yaml
from deepdiff import DeepDiff
from pprint import pprint

from tractor.etl_dsl.model import (
    ETLProject,
    CommandDetailed,
    StorageObjectDetailed,
    Task,
)


def test_minimal_etl_project_valid(minimal_etl_project, schema):
    """Test to validate the minimal ETL project against the provided schema.

    Args:
        minimal_etl_project (dict): The minimal ETL project data loaded from the YAML fixture.
        schema (dict): The JSON schema to validate the ETL project against.

    Raises:
        jsonschema.exceptions.ValidationError: If the ETL project does not conform to the schema.
    """
    jsonschema.validate(instance=minimal_etl_project, schema=schema)


def test_load_minimal_etl_project(minimal_etl_project):
    """Test to load the minimal ETL project into the ETLProject Pydantic model.

    Args:
        minimal_etl_project (dict): The minimal ETL project data loaded from the YAML fixture.

    Asserts:
        bool: True if the ETL project data matches the expected values.
    """
    print("expected")
    pprint(minimal_etl_project)

    etl_project = ETLProject(**minimal_etl_project)
    etl_project_dict = etl_project.model_dump(exclude_none=True)

    print("actual")
    pprint(etl_project_dict)
    # assert not DeepDiff(minimal_etl_project, etl_project_dict, ignore_order=True)

    assert etl_project.defaults
    assert etl_project.defaults.bucket_prefix == "my-prefix"
    assert etl_project.defaults.transformer

    assert etl_project.sources
    for s in etl_project.sources:
        assert s.extractor
        assert s.extractor.command
        command = s.extractor.command
        assert command.command == "echo Extracting data"
        assert s.extractor.outputs
        assert len(s.extractor.outputs) == 1
        output = s.extractor.outputs[0]
        assert isinstance(output, StorageObjectDetailed)
        assert output.url == "s3://my-bucket/my-prefix/extracted-data", output.url
        assert output.path == "extracted-data"
        assert output.type == "DIRECTORY"
        assert output.description == "Generated from extracted-data"

        assert s.transformer
        assert s.transformer.command
        command = s.transformer.command
        assert command.command == "echo Transforming data"


def test_load_minimal_etl_to_tes(minimal_etl_project):
    """Test to load the minimal ETL project into the ETLProject Pydantic model.

    Args:
        minimal_etl_project (dict): The minimal ETL project data loaded from the YAML fixture.

    Asserts:
        bool: True if the ETL project data matches the expected values.
    """

    etl_project = ETLProject(**minimal_etl_project)

    etl_project.apply_defaults()  # expand defaults

    pprint(etl_project)

    source = etl_project.sources[0]
    our_task = source.extractor
    assert isinstance(our_task.command, CommandDetailed)
    our_command: CommandDetailed = our_task.command
    tes_task = our_task.to_tes()
    print("actual")
    pprint(tes_task)
    assert tes_task.id == our_task.id
    assert tes_task.name == our_task.name
    assert len(tes_task.executors) == 1
    import shlex
    assert tes_task.executors[0].command == shlex.split(our_command.command)

    # TODO - make this more robust
    if our_command.image:
        assert tes_task.executors[0].image == our_command.image

    # check inputs and outputs
    if our_task.inputs:
        assert len(tes_task.inputs) == len(our_task.inputs)
        for i, _ in enumerate(our_task.inputs):
            _: StorageObjectDetailed = _
            assert _.url == tes_task.inputs[i].url
            assert "None" not in _.url
            assert etl_project.defaults.bucket_prefix in _.url
            assert _.path == tes_task.inputs[i].path
            assert _.type == tes_task.inputs[i].type

    if our_task.outputs:
        assert len(tes_task.outputs) == len(our_task.outputs)
        for i, _ in enumerate(our_task.outputs):
            _: StorageObjectDetailed = _
            assert _.url == tes_task.outputs[i].url
            assert "None" not in _.url
            assert etl_project.defaults.bucket_prefix in _.url
            assert _.path == tes_task.outputs[i].path
            assert _.type == tes_task.outputs[i].type

    # check datasets
    print("outlets", our_task.outlets)
    assert len(our_task.outlets) == 1
    assert our_task.outlets[0].uri == f"{source.id}-raw"


def test_load_etl_project(multi_source_etl_project, schema):
    """Test to load the multi-source ETL project into the ETLProject Pydantic model."""
    jsonschema.validate(instance=multi_source_etl_project, schema=schema)

    etl_project = ETLProject(**multi_source_etl_project)

    etl_project.apply_defaults()  # expand defaults

    pprint(etl_project)
    assert len(etl_project.sources) == len(multi_source_etl_project["sources"])
    assert len(etl_project.sources) == 2
    assert etl_project.loader
    assert isinstance(etl_project.loader.command, CommandDetailed)
    assert etl_project.loader.command.command == "echo Loading data"


def test_load_etl_load_pydantic_model(schema):
    """Test to load a task into the Task Pydantic model."""
    yaml_str = """
id: "load_1"
name: "Load Task"
command: "echo Loading data"
description: "Load data one"
inlets:
  - uri: "source_1-processed"
  - uri: "source_2-processed"
      """
    load_task = yaml.load(yaml_str, Loader=yaml.FullLoader)

    jsonschema.validate(instance=load_task, schema=schema)
    task = Task(**load_task)
    assert task


def test_tes_ping(tes_ping_project):
    """Test to load the TES ping project into the ETLProject Pydantic model."""
    etl_project = ETLProject(**tes_ping_project)

    etl_project.apply_defaults()  # expand defaults

    pprint(etl_project)
    assert len(etl_project.sources) == 1
    ping = etl_project.sources[0]
    extractor = ping.extractor
    assert len(extractor.outputs) == 1
    assert (
        extractor.outputs[0].url == "s3://foo/ping/service-info.json"
    ), extractor.outputs[0]

    transformer = ping.transformer
    assert len(transformer.inputs) == 1
    assert (
        transformer.inputs[0].url == "s3://foo/ping/service-info.json"
    ), transformer.inputs[0]
