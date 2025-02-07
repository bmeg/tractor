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

inlet_dataset = Dataset(f"{config.prefix}-processed")

@dag(
    dag_id=f"{config.prefix}-load_fhir",
    start_date=datetime(2024, 1, 1),
    schedule=[inlet_dataset],
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

    outlet_dataset = Dataset(f"{config.prefix}-fhir")

    @task()
    def read_manifest(inlet_dataset: Dataset):
        return {"manifest": inlet_dataset.extra["manifest"]}

    @task()
    def load_fhir_resources_task(config: Config, manifest):
        """Loads FHIR resources from NDJSON files in GCS into Google Healthcare FHIR."""
        try:
            pass
            # TODO - https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import
            # fhir_store_name = fhir_stores.build_fhir_store_name(
            #     project=config.project_id,
            #     location=config.location_id,
            #     dataset=inlet_dataset.id,
            #     fhir_store=config.fhir_store_id,
            # )
            #
            # client = healthcare_v1.FHIRClient(
            #     client_options={
            #         "api_endpoint": f"https://healthcare.googleapis.com/v1beta1/projects/{config.project_id}/locations/{config.location_id}"
            #     }
            # )
            #
            # for transformed_file in manifest:
            #     gcs_source = fhir_stores.GcsSource(
            #         uri=f"gs://{config.output_bucket}/{transformed_file}"
            #     )
            #
            #     request = fhir_stores.ImportResourcesRequest(
            #         parent=fhir_store_name,
            #         gcs_source=gcs_source,
            #         content_structure=fhir_stores.ContentStructure.RESOURCE,
            #     )
            #
            #     operation = client.import_resources(request=request)
            #     print(f"Import operation started: {operation.operation.name}")
            #     # ... (add code to check operation status until it is completed)...
            #     # result = operation.result()

        except Exception as e:
            print(f"Error loading FHIR resources: {e}")
            raise

    @task()
    def update_outlet_dataset(outlet_dataset: Dataset):
        outlet_dataset.extra = {}  # Update if you need to populate this.
        return outlet_dataset

    manifest = read_manifest(inlet_dataset)
    load_fhir_resources_task(config, manifest)
    update_outlet_dataset(outlet_dataset)


load_fhir_dag()
