import pytest
import airflow


@pytest.fixture
def airflow_url():
    """See https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#base-url"""
    # return "http://remote-airflow-instance:8080"
    return airflow.configuration.conf.get("webserver", "base_url")
