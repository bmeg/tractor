import pytest
from airflow import DAG
from tractor.etl_dsl.generators.fhir_aggregator import (
    fhir_aggregator_config,
    create_project,
    fhir_aggregator_dag,
)


def test_fhir_aggregator_dag_creation():
    config = fhir_aggregator_config()
    project = create_project(config)
    dags = fhir_aggregator_dag(project, scrape_bucket=True)
    print("dags:")
    print(dags)
    assert False, "TODO - implement test"
