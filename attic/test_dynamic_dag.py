import logging
import os
from unittest.mock import patch

import pytest
import responses
import yaml
from airflow.exceptions import AirflowSensorTimeout
from airflow.models import DagBag
from airflow.models import Variable
from airflow.sensors.http_sensor import HttpSensor

CONFIG_PATH = "/tmp/test_config.yaml"


@pytest.fixture
def mock_config():
    """Creates a temporary YAML config file for testing."""
    config_data = {
        "dag_id": "test_dynamic_dag",
        "schedule_dataset": "s3://test_schedule_dataset",
        "output_dataset": {
            "uri": "s3://test_output_dataset",
            "extra_attributes": {"owner": "data_team", "format": "csv"},
        },
        "website_check": {
            "url": "/healthcheck",
            "retries": 3,
            "timeout": 10,
        },
        "tags": ["test", "dataset-triggered"],
    }
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config_data, f)
    yield CONFIG_PATH
    os.remove(CONFIG_PATH)


@pytest.fixture
def dag(mock_config):
    """Generates a DAG dynamically from the test config."""
    # with patch.dict('os.environ', AIRFLOW_VAR_PROJECT_DIR=mock_config):
    #     value = Variable.get("PROJECT_DIR")
    #     assert value == mock_config
    #     from generate_dag_from_config import generate # see pytest.ini
    #     yield generate(mock_config)
    from generate_dag_from_config import generate  # see pytest.ini
    return generate(mock_config)


@pytest.fixture
def dag_bag(dag):
    """Loads DAG into a DAGBag for validation."""
    dag_bag = DagBag()
    dag_bag.dags[dag.dag_id] = dag
    return dag_bag


def test_dag_loaded(dag_bag):
    """Ensure DAG is correctly loaded."""
    assert "test_dynamic_dag" in dag_bag.dags
    dag = dag_bag.get_dag("test_dynamic_dag")
    assert dag is not None
    assert len(dag.tasks) == 2  # Ensure 2 tasks (sensor + processing)


def test_task_dependencies(dag):
    """Check task dependencies: HTTP Sensor -> Processing Task"""
    tasks = {task.task_id: task for task in dag.tasks}

    assert "check_website_availability" in tasks
    assert "process_data_task" in tasks

    http_sensor_task = tasks["check_website_availability"]
    process_data_task = tasks["process_data_task"]

    # Ensure the sensor task runs before the processing task
    assert process_data_task.upstream_task_ids == {"check_website_availability"}


@responses.activate
def test_http_sensor_success(dag):
    """Test successful HTTP sensor execution."""
    responses.add(
        responses.GET,
        "http://localhost/healthcheck",
        json={"status": "ok"},
        status=200,
    )

    # Get HTTP Sensor task
    http_sensor_task = next(
        (task for task in dag.tasks if isinstance(task, HttpSensor)), None
    )
    assert http_sensor_task is not None

    # Run task
    task_instance = http_sensor_task.execute(context={})
    assert task_instance is None  # Sensors return None on success


@responses.activate
def test_http_sensor_retries_and_logs(dag, caplog):
    """Test HTTP sensor with multiple failures before succeeding, ensuring retries and logging."""
    responses.add(
        responses.GET,
        "http://localhost/healthcheck",
        json={"status": "error"},
        status=500,
    )
    responses.add(
        responses.GET,
        "http://localhost/healthcheck",
        json={"status": "ok"},
        status=200,
    )

    # Capture logs
    with caplog.at_level(logging.INFO):
        # Get HTTP Sensor task
        http_sensor_task = next(
            (task for task in dag.tasks if isinstance(task, HttpSensor)), None
        )
        assert http_sensor_task is not None

        # Run task
        task_instance = http_sensor_task.execute(context={})

        # Check logs contain retry messages
        retry_logs = [
            record for record in caplog.records if "Retrying" in record.message
        ]
        assert len(retry_logs) > 0  # Ensure there are retry logs
        assert (
            "check_website_availability" in caplog.text
        )  # Task name should appear in logs


@responses.activate
def test_http_sensor_timeout(dag):
    """Test HTTP sensor timeout handling."""
    responses.add(
        responses.GET,
        "http://localhost/healthcheck",
        json={"status": "error"},
        status=500,
    )

    # Get HTTP Sensor task
    http_sensor_task = next(
        (task for task in dag.tasks if isinstance(task, HttpSensor)), None
    )
    assert http_sensor_task is not None

    # Expect timeout error after max retries
    with pytest.raises(AirflowSensorTimeout):
        http_sensor_task.execute(context={})


@patch("airflow.sensors.http_sensor.HttpSensor.log")
@responses.activate
def test_http_sensor_logging(mock_log, dag):
    """Test logging messages are generated correctly."""
    responses.add(
        responses.GET,
        "http://localhost/healthcheck",
        json={"status": "ok"},
        status=200,
    )

    # Get HTTP Sensor task
    http_sensor_task = next(
        (task for task in dag.tasks if isinstance(task, HttpSensor)), None
    )
    assert http_sensor_task is not None

    # Run task
    http_sensor_task.execute(context={})

    # Check logs were called
    mock_log.info.assert_any_call("Poking: %s", "/healthcheck")
    mock_log.info.assert_any_call("Success criteria met. Exiting.")
