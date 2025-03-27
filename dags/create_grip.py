from tractor.etl_dsl.generators.grip import grip_config, create_project, grip_dag
from airflow import DAG

# Create the grip DAG
config_ = grip_config()
dags: list[DAG] = grip_dag(create_project(config_))
for dag in dags:
    dag
