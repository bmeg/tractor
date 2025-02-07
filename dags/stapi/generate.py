import logging
import pathlib

from tractor.mkdag import generate
log = logging.getLogger(__name__)

configs = pathlib.Path("/opt/airflow/projects/stapi")
if not configs.exists():
    log.warning(f"No configs found in {configs}")
    exit()

generated_dags = []
for config in configs.glob("stapi-*.yaml"):
    generated_dags.extend(generate(str(config)))

for dag in generated_dags:
    log.info(f"Generated DAG: {dag}")
