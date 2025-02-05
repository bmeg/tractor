import pathlib

import pytest
import os
from click.testing import CliRunner
from mkdag.cli import render_dag_cli as render_dag


# Fixture for the valid YAML config path
@pytest.fixture
def valid_config():
    return "tests/fixtures/simple/dag_config.yaml"


# Fixture for the invalid YAML config path
@pytest.fixture
def invalid_config():
    return "tests/fixtures/simple/invalid_config.yaml"


# Fixture for the output directory
@pytest.fixture
def output_file():
    return "tests/output/simple"


# Fixture for the output directory
@pytest.fixture
def output_file_bad():
    return "tests/output/simple"


def test_render_dag_success(valid_config, output_file):
    """Test successful DAG rendering."""
    runner = CliRunner()
    result = runner.invoke(render_dag, [
        '--config', valid_config,
        '--output', output_file
    ])

    output_file = pathlib.Path(output_file)
    print(f"exit_code: {result.exit_code} {result.output}")
    assert result.exit_code == 0
    dag_path = output_file / "dags" / "simple_gene_processing_dag.py"
    assert os.path.exists(dag_path)
    with open(dag_path, 'r') as f:
        content = f.read()
        print(f"content: {content}")
        assert 'DAG' in content
        assert 'simple_gene_processing_dag' in content


def test_render_dag_invalid_config(invalid_config, output_file_bad):
    """Test rendering with invalid config."""
    runner = CliRunner()
    result = runner.invoke(render_dag, [
        '--config', invalid_config,
        '--output', output_file_bad
    ])

    print(f"exit_code: {result.exit_code} {result.output}")

    assert result.exit_code != 0
    assert "Error" in result.output
    # assert not os.path.exists(output_file_bad)


# def test_render_dag_no_output_option(valid_config):
#     """Test rendering without specifying output file (default name)."""
#     runner = CliRunner()
#     default_output = "generated_dag.py"
#     result = runner.invoke(render_dag, [
#         '--config', valid_config,
#     ])
#
#     assert result.exit_code == 0
#     assert os.path.exists(default_output)

    # # Cleanup
    # if os.path.exists(default_output):
    #     os.remove(default_output)


@pytest.fixture(scope="function", autouse=True)
def cleanup_output(output_file):
    """Cleanup generated output file after each test."""
    yield
    # if os.path.exists(output_file):
    #     os.remove(output_file)
