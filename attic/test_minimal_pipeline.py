import logging

import yaml
from airflow.models.dag import DAG

from tractor.etl_dsl_airflow import AggregateLoadDagGenerator
from tractor.etl_dsl_model import ETLProject

logger = logging.getLogger(__name__)
logger.warning("Starting test pipeline")
print("Starting test pipeline")
project_dict = yaml.load(
"""
# This is a test pipeline that will be used to test the Tractor pipeline generator.
id: "minimal_project"
name: "Minimal ETL Project"
description: "A minimal ETL project configuration."
defaults:
  bucket: "my-bucket"
  bucket_scheme: "s3"
  bucket_prefix: "my-prefix"
  operator_type: "BashOperator"
  callbacks:
    pre_execute: tractor.etl_dsl_callbacks.stub_pre_execute
    on_success: tractor.etl_dsl_callbacks.stub_on_success
    on_failure: tractor.etl_dsl_callbacks.stub_on_failure  
loader:
  id: "load_1"
  name: "Load Task"
  command: "echo Loading data"
  description: "Load data one"
  inlets:
    - uri: "source_1-processed"
    - uri: "source_2-processed"
sources:
  - id: "source_1"
    transformer:
      id: "transform_1"
      name: "Transform Task"
      command: "echo Transforming data"
      description: "Transform data 1"
      outputs:
        - source_1/processed-data
  - id: "source_2"
    transformer:
      id: "transform_2"
      name: "Transform Task"
      command: "echo Transforming data"
      description: "Transform data 2"
      outputs:
        - source_2/processed-data
""", Loader=yaml.SafeLoader)


def test_task_group_dag_generator(project: ETLProject) -> list[DAG]:
    logger.info(f"Generating DAGs for project: {project.id}")
    project.apply_defaults()
    return AggregateLoadDagGenerator(project=project).mkdags()


logger.info(f"Sourcing project: {project_dict['id']}")
dags = test_task_group_dag_generator(ETLProject(**project_dict))
logger.info(f"Generated DAGs: {dags}")
# render so airflow will pick it up
for _ in dags:
    _
