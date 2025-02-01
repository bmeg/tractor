import subprocess

import pytest
import os
from click.testing import CliRunner
from mkdag.cli import render_dag_cli as render_dag


# Fixture for the valid YAML config path
@pytest.fixture
def valid_config():
    return "tests/fixtures/pop-etl/config.yaml"


# Fixture for the Jinja template path
@pytest.fixture
def jinja_template():
    return "tests/fixtures/pop-etl/dag_template.jinja"


@pytest.fixture
def jinja_template_hook():
    return "tests/fixtures/pop-etl/hook_template.jinja"


# Fixture for the output directory
@pytest.fixture
def output_file():
    return "tests/output/pop-etl/generated_dag.py"


@pytest.fixture
def output_file_hook():
    return "tests/output/pop-etl/generated_hook.py"


# Fixture for the output directory
@pytest.fixture
def output_file_bad():
    return "tests/output/pop-etl/generated_dag-bad.py"


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
        assert 'test2_dag' in content


def test_render_hook_success(valid_config, jinja_template_hook, output_file_hook):
    """Test successful DAG rendering."""
    runner = CliRunner()
    result = runner.invoke(render_dag, [
        '--config', valid_config,
        '--template', jinja_template_hook,
        '--output', output_file_hook
    ])

    print(f"exit_code: {result.exit_code} {result.output}")
    assert result.exit_code == 0
    assert os.path.exists(output_file_hook)
    with open(output_file_hook, 'r') as f:
        content = f.read()
        print(f"content: {content}")
        assert 'Test2Hook' in content

    # Run black to test formatting
    black_result = subprocess.run(['black', '--check', '--diff', output_file_hook], capture_output=True, text=True)
    print(f"black exit_code: {black_result.returncode} {black_result.stdout} {black_result.stderr}")
    assert black_result.returncode == 0, f"Black formatting check failed: {black_result.stdout} {black_result.stderr}"


@pytest.fixture(scope="function", autouse=True)
def cleanup_output(output_file):
    """Cleanup generated output file after each test."""
    yield
    # if os.path.exists(output_file):
    #     os.remove(output_file)
