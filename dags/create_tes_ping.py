import logging

import yaml
from airflow import DAG

from tractor.etl_dsl.generators.dag_generator import AggregateLoadDagGenerator
from tractor.etl_dsl.model import ETLProject

log = logging.getLogger(__name__)


def tes_config():
    """
    Test configuration for the funnel testing project.
    """

    _config = yaml.safe_load(
        """
id: test-tes
defaults:
  # TODO: add default bucket, coordinate with TES's bucket
  bucket: foo
sources:
- id: ping
  name: ping
  extractor:
      id: ping-extractor
      name: ping-extractor
      description: Get the service-info from the funnel service via BashOperator curl
      command:
        command: curl -s -X GET curl http://host.docker.internal:8000/service-info
        operator_type: BashOperator
    
  transformer:
      id: ping-transformer
      name: ping-transformer
      description: Say hello world via TESOperator echo
      command: 
        # TODO - add outputs
        command: echo hello
        operator_type: TESOperator

    """
    )
    return _config


# Create the grip DAG
config_ = tes_config()
dags: list[DAG] = AggregateLoadDagGenerator(project=ETLProject.create_project(config_)).mkdags()
log.info(f"tes-ping-dags: {[dag.dag_id for dag in dags]}")
for dag in dags:
    dag

