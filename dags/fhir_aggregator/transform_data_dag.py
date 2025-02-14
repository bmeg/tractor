from __future__ import annotations

import logging
import os
import shutil
import tempfile
from datetime import datetime
from typing import List

from airflow.datasets import Dataset
from airflow.decorators import task, dag
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage

from fhir_aggregator.config import Config, load_default_config
log = logging.getLogger(__name__)

try:
    config = load_default_config()
except Exception as e:
    log.error(f"Error loading config : {e}")
    exit(0)


# def transform_datasets(inlet_events, outlet_events, input_datasets, output_datasets):
#     output_manifest = []
#     for dataset in input_datasets:
#         logging.info(f"Processing dataset: {dataset}")
#         events = inlet_events[dataset]
#         if events is None or len(events) == 0:
#             logging.warning(f"No events found for {dataset}")
#             continue
#         last_event = events[-1]
#         assert last_event, f"No events found for {dataset}"
#         assert "manifest" in last_event.extra, f"No manifest found in {last_event}"
#         manifest = last_event.extra["manifest"]
#         logging.info(f"Extracted data: {manifest}")
#         output_manifest.extend(manifest)
#     outlet_events[output_datasets].extra = {"manifest": output_manifest}
#     return output_manifest

dags = []

for project in config.projects:
    prefix = project.id

    @dag(
        dag_id=f"{prefix}-transform",
        start_date=datetime(2024, 1, 1),
        schedule=[Dataset(f"{prefix}-raw")],  # Schedule based on inlet dataset
        catchup=False,
        render_template_as_native_obj=True,
        tags=["transform", prefix],
        doc_md="""# Transform Data DAG
    
    This DAG reads data from a local directory, performs transformations, and uploads the transformed data to a Google Cloud Storage bucket.  It creates a new manifest of the transformed files and stores it in the dataset's extra field.  It is triggered by the completion of the `ingest_data_dag`'s raw dataset. It uses the Config class for configuration. It uses TaskFlow API and inlet/outlet datasets.
    
    **Configuration:**
    
    *   Reads configuration from the config file specified in `dag_run.conf['config_path']`.
    
    **Functionality:**
    
    1.  Triggered when the `<ingest_data_dag>-raw` dataset is updated.
    2. Reads the manifest of ingested files from the upstream DAG's inlet dataset.
    3.  Downloads the selected files to a local directory.
    4.  Performs transformations via a bash command
    5.  On success, updates the Airflow dataset with the transformed file manifest and uploads the transformed files to the specified GCS bucket.
    6.  Updates the Airflow outlet dataset with the manifest of transformed files.
    
    **Considerations:**
    
    *   Increase templating, logging, and error handling for production use.
    *   Error handling and logging should be enhanced for production use.
    *   The input directory is hardcoded as `/tmp/data`.  Change as necessary.
    """,
    )
    def transform_dag():

        # create local variables private to the DAG
        inlet_dataset = Dataset(f"{prefix}-raw")
        outlet_dataset = Dataset(f"{prefix}-processed")

        project_id = project.id
        # convention is that the project id is used as a prefix aka folder
        bucket_prefix = project.id
        # each project can have its own bucket
        project_bucket = project.bucket
        # use our own temp dir
        tempdir = tempfile.mkdtemp()

        def list_files_recursive(directory) -> list[str]:
            """List all files in a directory and its subdirectories."""
            file_paths = []
            for root, dirs, files in os.walk(directory):
                for file in files:
                    file_paths.append(str(os.path.join(root, file)).replace(directory + '/', '', 1))
            return file_paths

        def on_failure_callback(context):
            """Alert the team on failure, clean up the temp dir."""
            cwd = os.getcwd()
            state = context['ti'].state
            print(f"on_failure_callback:\nstate: {state}\ncwd: {cwd}\ncontext: {context}\nti: {context['ti']}")
            # clean up the temp dir
            shutil.rmtree(tempdir)
            log.info(f"Removed {tempdir}")

        def on_success_callback(context):
            """Update the outlets with the manifest, clean up the temp dir."""
            cwd = os.getcwd()
            log.info(f"on_success_callback\ncwd: {cwd}\ncontext: {context}\nti: {context['ti']}")
            # update the outlets with the manifest
            _outlets = context['task'].outlets
            dir_name = os.path.join(tempdir, "OUTPUT")
            manifest = [f"OUTPUT/{_}" for _ in list_files_recursive(dir_name)]
            extra = {"manifest": manifest}
            hook = GCSHook()
            log.info(f"Uploading manifest to {project_bucket} {bucket_prefix}")
            for file_name in manifest:
                object_name = file_name.replace("OUTPUT/", "TESTING-OUTPUT/")
                hook.upload(bucket_name=project_bucket, object_name=object_name, filename=file_name)

            # upload the manifest to GCS
            for outlet in _outlets:
                context["outlet_events"][outlet].extra = extra
                log.info(f"Updated outlet {outlet} with extra: {extra}")
            # clean up the temp dir
            shutil.rmtree(tempdir)
            log.info(f"Removed {tempdir}")

        def pre_execute(context):
            """Change to the temp dir before executing the command."""
            os.chdir(tempdir)
            hook = GCSHook()
            manifest = hook.list(bucket_name=project_bucket, prefix=bucket_prefix)
            log.info(f"pre_execute listing {project_bucket} {bucket_prefix} {manifest}")
            for object_name in manifest:
                file_name = os.path.join(tempdir, object_name)
                # Ensure the directory for file_name exists
                os.makedirs(os.path.dirname(file_name), exist_ok=True)
                # download the data
                location = hook.download(bucket_name=project_bucket, object_name=object_name, filename=file_name)
                log.info(f"Downloaded {object_name} to {file_name} at location {location}")

        @task.bash(
            task_id='transform',
            cwd=tempdir,
            inlets=[inlet_dataset],
            outlets=[outlet_dataset],
            pre_execute=pre_execute,
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            retries=0,
        )
        def transform():
            """Reads the manifest from the inlet dataset."""
            return f"pwd;fa_submit prep {bucket_prefix}/META OUTPUT/R4/{bucket_prefix}/META --transformers part-of,vocabulary,validate --fhir-version R4"

        transform()

    dags.append(transform_dag())

dags
