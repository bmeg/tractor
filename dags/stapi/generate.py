import logging
import pathlib

from tractor.mkdag import generate

configs = pathlib.Path("/opt/airflow/projects/stapi")
generated_dags = []
for config in configs.glob("stapi-*.yaml"):
    generated_dags.extend(generate(str(config)))

for dag in generated_dags:
    logging.info(f"Generated DAG: {dag}")
