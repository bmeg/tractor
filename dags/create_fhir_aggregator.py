from tractor.etl_dsl.generators.fhir_aggregator import fhir_aggregator_config, create_project, fhir_aggregator_dag
from airflow import DAG

# Create the FHIR Aggregator DAG
config_ = fhir_aggregator_config()
dags: list[DAG] = fhir_aggregator_dag(create_project(config_))
for dag in dags:
    dag
