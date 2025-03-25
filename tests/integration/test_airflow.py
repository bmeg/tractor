import requests
import pytest


@pytest.mark.integration
def test_airflow_connection(airflow_url):
    """Test connection to the remote Airflow instance."""
    assert airflow_url is not None, "Airflow URL is not set"
    response = requests.get(f"{airflow_url}/health")
    assert response.status_code == 200, "Failed to connect to the remote Airflow instance"
    health_status = response.json()
    assert health_status["metadatabase"]["status"] == "healthy", "Airflow metadatabase is not healthy"
    assert health_status["scheduler"]["status"] == "healthy", "Airflow scheduler is not healthy"

#
#
# # Create an instance of the DAG API
# dag_api_instance = dag_api.DAGApi(airflow_client.client.ApiClient(configuration))
#
# # Create an instance of the DAG Run API
# dag_run_api_instance = dag_run_api.DAGRunApi(airflow_client.client.ApiClient(configuration))
#
# # Example: List all DAGs
# try:
#     api_response = dag_api_instance.get_dags()
#     print(api_response)
# except airflow_client.client.ApiException as e:
#     print("Exception when calling DAGApi->get_dags: %s\n" % e)
#
# # Example: Trigger a DAG run
# dag_id = "example_dag"
# dag_run_id = "example_dag_run"
# try:
#     dag_run = dag_run_api_instance.post_dag_run(dag_id, {"dag_run_id": dag_run_id})
#     print(dag_run)
# except airflow_client.client.ApiException as e:
#     print("Exception when calling DAGRunApi->post_dag_run: %s\n" % e)