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


# Fixture for the Jinja template path
@pytest.fixture
def jinja_template():
    return "tests/fixtures/simple/dag_template.jinja"


# Fixture for the output directory
@pytest.fixture
def output_file():
    return "tests/output/simple/generated_dag.py"


# Fixture for the output directory
@pytest.fixture
def output_file_bad():
    return "tests/output/simple/generated_dag-bad.py"


def test_render_dag_success(valid_config, jinja_template, output_file):
    """Test successful DAG rendering."""
    runner = CliRunner()
    result = runner.invoke(render_dag, [
        '--config', valid_config,
        '--template', jinja_template,
        '--output', output_file
    ])

    print(f"exit_code: {result.exit_code} {result.output}")
    assert result.exit_code == 0
    assert os.path.exists(output_file)
    with open(output_file, 'r') as f:
        content = f.read()
        print(f"content: {content}")
        assert 'DAG' in content
        assert 'test_dag' in content


def test_render_dag_invalid_config(invalid_config, jinja_template, output_file_bad):
    """Test rendering with invalid config."""
    runner = CliRunner()
    result = runner.invoke(render_dag, [
        '--config', invalid_config,
        '--template', jinja_template,
        '--output', output_file_bad
    ])

    print(f"exit_code: {result.exit_code} {result.output}")

    assert result.exit_code != 0
    assert "Error" in result.output
    assert not os.path.exists(output_file_bad)


def test_render_dag_missing_template(valid_config):
    """Test rendering with a non-existent template."""
    runner = CliRunner()
    result = runner.invoke(render_dag, [
        '--config', valid_config,
        '--template', 'non_existent_template.jinja',
        '--output', 'tests/output/non_existent_output.py'
    ])

    assert result.exit_code != 0
    assert "Error" in result.output


def test_render_dag_no_output_option(valid_config, jinja_template):
    """Test rendering without specifying output file (default name)."""
    runner = CliRunner()
    default_output = "generated_dag.py"
    result = runner.invoke(render_dag, [
        '--config', valid_config,
        '--template', jinja_template
    ])

    assert result.exit_code == 0
    assert os.path.exists(default_output)

    # Cleanup
    if os.path.exists(default_output):
        os.remove(default_output)


@pytest.fixture(scope="function", autouse=True)
def cleanup_output(output_file):
    """Cleanup generated output file after each test."""
    yield
    # if os.path.exists(output_file):
    #     os.remove(output_file)
