import logging
import pathlib

from tractor.mkdag import generate
log = logging.getLogger(__name__)

config_path = "/opt/airflow/projects/stapi"

configs = pathlib.Path(config_path)
if not configs.exists():
    log.warning(f"No configs found in {config_path}")
    exit()

generated_dags = []
for config in configs.glob("stapi-*.yaml"):
    generated_dags.extend(generate(str(config)))

for dag in generated_dags:
    log.info(f"Generated DAG: {dag}")
