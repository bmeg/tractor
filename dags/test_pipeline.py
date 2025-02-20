import logging

import yaml
from airflow.models.dag import DAG

from tractor.config import Project
from tractor.dag_generator import AggregateLoadDagGenerator

logger = logging.getLogger(__name__)
logger.warning("Starting test pipeline")
print("Starting test pipeline")
project_dict = yaml.load(
"""
# This is a test pipeline that will be used to test the Tractor pipeline generator.
id: test5
name: Test 3 Project
commands: 
    - echo "load-all"

defaults:
    bucket: bucket1
    bucket_scheme: s3
    operator_type: BashOperator
    commands:
        - echo "extract"
        - echo "transform"
sources:
    - id: tcga
      name: TCGA ETL      
      inputs:
        - input/tcga/file1.txt
      outputs:
        - output/tcga/file1.json
    - id: gcd
      name: GCD ETL
      inputs:
        - input/gcd/file1.txt
      outputs:
        - output/gcd/file1.json

""", Loader=yaml.SafeLoader)


# def project() -> Project:
#     defaults = Default(**{"bucket": "bucket1", "bucket_scheme": "s3", "command": "echo hello"})
#     defaults.command = {
#         "data": {
#             "image": "airflow-operator://PythonOperator",
#             "command": "print",
#         }
#     }
#
#     project = Project(
#         id="test2",
#         defaults=defaults,
#         sources=[
#             DagConfig(**{
#                 "id": "tcga",
#                 "name": "TCGA ETL",
#                 "inputs": ["input/file1.txt", "input/file2.txt", "input/dir1"],
#                 "outputs": ["output/file1.txt", "output/file2.txt", "output/dir1"],
#             }, defaults=defaults),
#             DagConfig(**{
#                 "id": "gcd",
#                 "name": "GCD ETL",
#                 "inputs": ["input/file1.txt", "input/file2.txt", "input/dir1"],
#                 "outputs": ["output/file1.txt", "output/file2.txt", "output/dir1"]
#             }, defaults=defaults),
#         ],
#     )
#     return project




def test_task_group_dag_generator(project) -> list[DAG]:
    logger.info(f"Generating DAGs for project: {project}")
    return AggregateLoadDagGenerator().mkdags(project)


logger.warning(f"Sourcing project: {project_dict}")
dags = test_task_group_dag_generator(Project(**project_dict))
logger.warning(f"Generated DAGs: {dags}")
for _ in dags:
    _
