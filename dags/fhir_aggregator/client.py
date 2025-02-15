import base64
import os
import subprocess
import time

import click
import requests
import json


def _get_base_url():
    """Get the base URL for the Airflow API."""
    # TODO https://cloud.google.com/composer/docs/access-airflow-api#rest-api-versions
    return os.getenv("AIRFLOW_API_URL", "http://localhost:8080")


def _create_headers(username, password):
    """Create headers. Assumes google token unless username, password set."""

    auth_type = "token"
    if any([username, password]):
        auth_type = "basic"

    if auth_type == "token":
        AIRFLOW_API_TOKEN = os.getenv("AIRFLOW_API_TOKEN", None)
        if not AIRFLOW_API_TOKEN:
            print("No API token found, attempting to get one from gcloud")

            # Get the API token by executing the gcloud command
            def get_gcloud_token():
                result = subprocess.run(
                    ["gcloud", "auth", "print-access-token"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if result.returncode != 0:
                    raise Exception(f"Error getting token: {result.stderr}")
                return result.stdout.strip()

            AIRFLOW_API_TOKEN = get_gcloud_token()
            print("API token retrieved successfully", AIRFLOW_API_TOKEN)

        # Headers for the request
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {AIRFLOW_API_TOKEN}",  # Replace with your actual API token
        }
    else:

        # Basic Auth credentials
        username = os.getenv("AIRFLOW_API_USERNAME", "gtex")
        password = os.getenv("AIRFLOW_API_PASSWORD", "gtex")

        # Encode credentials in Base64
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        # Headers for the request
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {encoded_credentials}",
        }
    return headers


def _start_dag_run(dag_id, headers):
    """Start a DAG run using the Airflow API."""

    # Data payload for the request
    data = {}
    # Airflow API endpoint
    airflow_url = f"{_get_base_url()}/api/v1/dags/{dag_id}/dagRuns"

    # Make the POST request to trigger the DAG
    response = requests.post(airflow_url, headers=headers, data=json.dumps(data))

    # Check the response
    if response.status_code != 200:
        print(f"Failed to trigger DAG: {response.status_code} - {response.text}")
        exit(1)

    print(f"DAG {dag_id} triggered successfully")

    dag_run = response.json()
    assert "dag_run_id" in dag_run, dag_run
    dag_run_id = dag_run["dag_run_id"]
    return dag_run_id


def _wait_for_completion(dag_id, dag_run_id, headers) -> (str, dict):
    """Poll the Airflow API for the status of the DAG run."""

    dag_run_url = f"{_get_base_url()}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
    dag_run = {}
    state = None
    sleep_time = 2
    max_sleep_time = 10
    time.sleep(sleep_time)  # Wait 2 seconds before polling for the status
    while True:
        dag_run_response = requests.get(dag_run_url, headers=headers)
        if dag_run_response.status_code == 200:
            dag_run = dag_run_response.json()
            state = dag_run["state"]
            if state in ["success", "failed"]:
                break
            else:
                print(f"DAG run status: {state}")
        else:
            print(
                f"Failed to get DAG run status: {dag_run_response.status_code} - {dag_run_response.text}"
            )
            break
        sleep_time = min(2 * sleep_time, max_sleep_time)
        time.sleep(sleep_time)  # Wait for max_sleep_time seconds before polling again

    return state, dag_run


def _get_logs(dag_id, dag_run_id, headers):
    """Get the logs for the tasks in the DAG run."""
    task_instances_url = (
        f"{_get_base_url()}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    )
    task_instances_response = requests.get(task_instances_url, headers=headers)
    if task_instances_response.status_code == 200:
        task_instances = task_instances_response.json()["task_instances"]
        for task_instance in task_instances:
            task_id = task_instance["task_id"]
            task_state = task_instance["state"]
            if task_state in ['upstream_failed']:
                print(f"task_instance {task_id} not scheduled - {task_state}")
                continue
            print(f"task_instance {task_id} - {task_state}")
            log_url = f"{_get_base_url()}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/1"
            log_response = requests.get(log_url, headers=headers)
            if log_response.status_code == 200:
                print(f"Logs for task {task_id}:")
                print(log_response.text)
                if task_state == "failed":
                    print(f"Task {task_id} failed, skipping downstream tasks")
                    continue
                downstream_url = f"{_get_base_url()}/api/v1/dags/{dag_id}/tasks/{task_id}/downstream"
                downstream_response = requests.get(downstream_url, headers=headers)
                if downstream_response.status_code == 200:
                    downstream = downstream_response.json()
                    print(f"Downstream tasks for {task_id}: {downstream}")
                else:
                    print(
                        f"Failed to get downstream tasks for task {task_id}: {downstream_response.status_code} - {downstream_response.text}"
                    )

            else:
                print(
                    f"Failed to get logs for task {task_id}: {log_response.status_code} - {log_response.text}"
                )
    else:
        print(
            f"Failed to get task instances: {task_instances_response.status_code} - {task_instances_response.text}"
        )


def _downstream_datasets(dag_id, execution_date_gte, headers):
    """Get downstream datasets for the DAG."""

    datasets_url = f"{_get_base_url()}/api/v1/datasets?dag_ids={dag_id}"
    datasets_response = requests.get(datasets_url, headers=headers)
    if datasets_response.status_code == 200:
        datasets = datasets_response.json()
        for dataset in datasets["datasets"]:
            consuming_dags = dataset.get("consuming_dags", [])
            # print(f"Consuming DAGs for dataset {dataset['uri']}: {consuming_dags}")
            for consuming_dag in consuming_dags:
                consuming_dag_id = consuming_dag["dag_id"]
                print(f"DAG {consuming_dag_id} consumes {dataset['uri']}:")
                execution_date_gte = execution_date_gte.replace("+00:00", "Z")
                time.sleep(2)
                max_retries = 10
                retries = 0
                while True:
                    downstream_dag_url = f"{_get_base_url()}/api/v1/dags/{consuming_dag_id}/dagRuns?execution_date_gte={execution_date_gte}"
                    downstream_dag_response = requests.get(
                        downstream_dag_url, headers=headers
                    )
                    if downstream_dag_response.status_code == 200:
                        downstream_dag_runs = downstream_dag_response.json()["dag_runs"]
                        has_runs = len(downstream_dag_runs) > 0
                        for dag_run in downstream_dag_runs:
                            # print(f"DAG run: {dag_run['state']} - {dag_run['execution_date']}: ")
                            if dag_run["state"] == "success":
                                print(
                                    f"  Dataset {dataset['uri']} consumed by DAG {consuming_dag_id} on {dag_run['execution_date']}"
                                )
                            else:
                                print(
                                    f"  Dataset {dataset['uri']} not consumed by DAG {consuming_dag_id} on {dag_run['execution_date']}"
                                )
                                _get_logs(
                                    consuming_dag_id, dag_run["dag_run_id"], headers
                                )
                        if has_runs:
                            break
                    else:
                        print(
                            f"Failed to get DAG runs for consuming DAG {consuming_dag_id}: {downstream_dag_response.status_code} - {downstream_dag_response.text}\n{downstream_dag_url}"
                        )
                    time.sleep(10)
                    retries += 1
                    if retries >= max_retries:
                        break

    else:
        print(
            f"Failed to get datasets: {datasets_response.status_code} - {datasets_response.text}"
        )


@click.command()
@click.option('--project_id', default=None, help='Project ID')
@click.option('--dag_id', default=None, help='DAG ID, defaults to {project_id}-extract')
@click.option('--username', default=lambda: os.getenv("AIRFLOW_API_USERNAME", None), help='Airflow API username. AIRFLOW_API_USERNAME')
@click.option('--password', default=lambda: os.getenv("AIRFLOW_API_PASSWORD", None), help='Airflow API password. AIRFLOW_API_PASSWORD')
@click.option('--debug', default=True, help='Debug mode')
def cli(project_id: str, dag_id: str, username: str, password: str, debug: bool):
    """CLI tool to trigger a DAG and get downstream datasets. Uses Google token by default, set username and password for basic auth.
    """
    try:
        if not project_id:
            raise ValueError("Project ID is required. See https://github.com/bmeg/tractor/blob/6b146449b658fb8660aaaaef60baf7c44a9b55fd/projects/fhir-aggregator/load_fhir_dag.py#L10")
        if not dag_id:
            dag_id = f"{project_id}-extract"

        headers = _create_headers(username, password)
        dag_run_id = _start_dag_run(dag_id, headers)
        state, dag_run = _wait_for_completion(dag_id, dag_run_id, headers)
        if state == "failed":
            _get_logs(dag_id, dag_run_id, headers)
            exit(1)

        # the dag run was successful, print the dataset events
        execution_date_gte = dag_run["execution_date"]

        print(f"DAG {dag_id} run {dag_run_id} was successful!")
        print("Dataset events:")
        _downstream_datasets(dag_id, execution_date_gte, headers)
    except Exception as e:
        print(f"An error occurred: {e}")
        if debug:
            import traceback
            traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    cli()
