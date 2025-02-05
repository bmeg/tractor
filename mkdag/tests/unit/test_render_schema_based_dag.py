import ast
import subprocess

import pytest
import yaml

from mkdag.dag_generator import DAGGenerator, HookGenerator


# Fixture for the valid YAML config path
@pytest.fixture
def valid_config() -> str:
    return "tests/fixtures/schema-based/config.yaml"


@pytest.fixture
def valid_config_dict(valid_config) -> dict:
    return yaml.load(open(valid_config), yaml.SafeLoader)


@pytest.fixture
def no_schedule_config_dict() -> dict:
    return yaml.load(open("tests/fixtures/schema-based/config-no-schedule.yaml"), yaml.SafeLoader)


# Fixture for the output directory
@pytest.fixture
def output_file():
    return "tests/output/schema-based/generated_dag.py"


@pytest.fixture
def output_file_hook():
    return "tests/output/schema-based/generated_hook.py"


def test_render_dag_decorator(valid_config_dict: dict, output_file: str):
    """Test successful DAG rendering."""
    dag = DAGGenerator(valid_config_dict).render()
    print(dag)
    # Check if the generated DAG code is valid Python code
    try:
        ast.parse(dag)
    except SyntaxError as e:
        raise AssertionError(f"Generated DAG code is not valid Python code: {e}")

    with open(output_file, 'wt') as f:
        f.write(dag)

    # Run black to test formatting
    # black_result = subprocess.run(['black', '--check', '--diff', output_file], capture_output=True, text=True)
    black_result = subprocess.run(['black',  output_file], capture_output=True, text=True)
    print(f"black exit_code: {black_result.returncode} {black_result.stdout} {black_result.stderr}")
    assert black_result.returncode == 0, f"Black formatting check failed: {black_result.stdout} {black_result.stderr}"


def test_render_dag_no_schedule(no_schedule_config_dict: dict, output_file: str):
    """Test successful DAG rendering."""
    dag = DAGGenerator(no_schedule_config_dict).render()
    print(dag)
    # Check if the generated DAG code is valid Python code
    try:
        ast.parse(dag)
    except SyntaxError as e:
        raise AssertionError(f"Generated DAG code is not valid Python code: {e}")

    with open(output_file, 'wt') as f:
        f.write(dag)

    # Run black to test formatting
    # black_result = subprocess.run(['black', '--check', '--diff', output_file], capture_output=True, text=True)
    black_result = subprocess.run(['black',  output_file], capture_output=True, text=True)
    print(f"black exit_code: {black_result.returncode} {black_result.stdout} {black_result.stderr}")
    assert black_result.returncode == 0, f"Black formatting check failed: {black_result.stdout} {black_result.stderr}"


def test_render_hook(valid_config_dict: dict, output_file_hook: str):
    """Test successful Hook rendering."""
    hook = HookGenerator(valid_config_dict).render()
    print(hook)
    with open(output_file_hook, 'wt') as f:
        f.write(hook)

    # Check if the generated DAG code is valid Python code
    try:
        ast.parse(hook)
    except SyntaxError as e:
        raise AssertionError(f"Generated DAG code is not valid Python code: {e}")


    # Run black to test formatting
    black_result = subprocess.run(['black', '--check', '--diff', output_file_hook], capture_output=True, text=True)
    print(f"black exit_code: {black_result.returncode} {black_result.stdout} {black_result.stderr}")
    assert black_result.returncode == 0, f"Black formatting check failed: {black_result.stdout} {black_result.stderr}"


@pytest.fixture(scope="function", autouse=True)
def cleanup_output(output_file, output_file_hook):
    """Cleanup generated output file after each test."""
    yield
    # for output_file in [output_file, output_file_hook]:
    #     if os.path.exists(output_file):
    #         os.remove(output_file)
