from __future__ import annotations

import logging
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import task, dag

# from google.cloud import healthcare_v1
# from google.cloud.healthcare_v1 import fhir_stores

from fhir_aggregator.config import load_default_config, Config

log = logging.getLogger(__name__)

try:
    config = load_default_config()
except Exception as e:
    log.error(f"Error loading config : {e}")
    exit(0)


inlet_datasets = []
for project in config.projects:
    inlet_datasets.append(Dataset(f"{project.id}-processed"))

website = Dataset(
    f"website-{config.fhir.project_id}/{config.fhir.fhir_store_id}-static_files"
)


@dag(
    dag_id=f"load_fhir",
    start_date=datetime(2024, 1, 1),
    schedule=inlet_datasets,
    catchup=False,
    tags=["fhir"],
    render_template_as_native_obj=True,
    doc_md="""# Load FHIR DAG

This DAG loads transformed data from Google Cloud Storage into a Google Healthcare FHIR service. It is triggered by the completion of the `transform_data_dag`. It uses the TaskFlow API, an outlet dataset, and the Config class for configuration.

**Configuration:**

*   Reads configuration from the config file specified in `dag_run.conf['config_path']`.

**Functionality:**

1.  Reads the manifest of transformed files from the upstream DAG's outlet dataset.
2.  Iterates through the transformed files.
3.  Constructs a FHIR import request for each file using the `RESOURCE` content structure.
4.  Initiates an asynchronous FHIR import operation for each file.
5.   **(MISSING):**  The DAG currently lacks a mechanism to check the status of the asynchronous import operations.  This is essential to ensure data is properly loaded.


**Considerations:**

*   The asynchronous import operations need to be monitored; the current code only starts the operations. Add a proper mechanism to wait for completion and handle potential errors.
*   The `fhir_store_id`, `project_id`, and `location_id` must be specified correctly in the configuration.
*   Ensure that the service account has the necessary permissions to access GCS and Google Healthcare FHIR.
""",
)
def load_fhir_dag(**kwargs):

    def _load(manifest) -> dict:
        """Transforms data and creates a manifest of transformed files."""
        try:
            for _ in manifest["manifest"]:
                log.info(f"Loading {_}")
            return {"loaded": manifest["manifest"]}

        except Exception as e:
            print(f"Error loading data: {e}")
            return {}

    @task(inlets=inlet_datasets)
    def load(inlet_events):
        """Reads the manifest from the inlet dataset."""
        loaded_manifests = []
        for inlet_dataset in inlet_datasets:
            events = inlet_events[inlet_dataset]
            assert len(events) > 0, "Should have at least 1 event"
            inlet_event = events[-1]
            assert (
                "manifest" in inlet_event.extra
            ), f"manifest not found in inlet_event {inlet_event.extra}"
            loaded_manifests.append(_load({"manifest": inlet_event.extra["manifest"]}))
        return loaded_manifests

    @task(outlets=[website])
    def generate_website(outlet_events):
        log.info(f"Loading {config.fhir}")
        outlet_events[website].extra = {"message": f"loaded {website.uri}"}

    load() >> generate_website()


load_fhir_dag()
