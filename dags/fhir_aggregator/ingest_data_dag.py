from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import task, dag
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowException

from fhir_aggregator.config import load_default_config, Config

log = logging.getLogger(__name__)

try:
    config = load_default_config()
except Exception as e:
    log.error(f"Error loading config : {e}")
    exit(0)


@dag(
    dag_id=f"{config.prefix}-extract",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    doc_md="""# Ingest Data DAG

This DAG ingests data from a Google Cloud Storage bucket, creates a manifest of the ingested files, and updates the Airflow dataset with the manifest. It uses the TaskFlow API and an outlet dataset.  It only reads objects with the specified prefix. The manifest is a simple list of file paths.  The configuration is loaded from a JSON or YAML file specified in `dag_run.conf['config_path']`. This configuration file is typically provided when triggering the DAG run via the Airflow UI, CLI, or API.


**Configuration:**

*   The DAG reads configuration from a file specified via `dag_run.conf['config_path']`.
*   dag_run.conf is not a file or a pre-defined setting; it's a dynamic dictionary constructed when a DAG run is initiated, based on the configuration provided through the chosen trigger mechanism. The tasks within the DAG access this information using Jinja templating ( {{ dag_run.conf['key'] }} ).
*   The config file should be a valid JSON or YAML file and include a `prefix` field.

**Functionality:**

1.  Reads configuration from the specified config file.
2.  Lists the contents of the specified GCS bucket, filtering by prefix.
3.  Creates a manifest containing only the file paths of the matching files.
4.  Updates the Airflow dataset (outlet) with the generated manifest.
5.  Downloads the selected files to a local directory.

**Considerations:**

*   Ensure the service account used by Airflow has the necessary permissions to access GCS.
*   The `/tmp/data` directory is used to store downloaded files. Consider using a more robust and managed location for production environments.

""",
)
def ingest_data_dag(**kwargs):

    outlet_dataset = Dataset(f"{config.prefix}-raw")

    @task(outlets=[outlet_dataset])
    def create_and_update_dataset_task(outlet_events, *args, **kwargs):
        """Creates a manifest of files in GCS, filtering by prefix, and updates the dataset."""
        try:
            manifest = [_ for _ in config.expected_files]
            outlet_events[outlet_dataset].extra = {"manifest": manifest}
            return {"manifest": manifest}
        except Exception as e:
            log.exception(f"Error creating manifest and updating dataset: {e}")
            raise AirflowException(f"Error creating manifest and updating dataset: {e}")

    @task(inlets=[outlet_dataset])
    def download_files_task(inlet_events):
        """Downloads files from GCS."""
        download_dir = "/tmp/data"
        os.makedirs(download_dir, exist_ok=True)
        extra = inlet_events[outlet_dataset][-1].extra
        for file_path in extra["manifest"]:
            try:
                file_name = file_path.replace(f"gs://{config.bucket}/", "")
                log.info(f"Downloaded (mock) {file_path} {file_name}")
            except Exception as e:
                log.exception(f"Error downloading {file_path}: {e}")
                raise AirflowException(f"Error downloading {file_path}: {e}")

    create_and_update_dataset_task() >> download_files_task()


ingest_data_dag()