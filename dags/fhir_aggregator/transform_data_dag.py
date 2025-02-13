from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import task, dag
from google.cloud import storage

from fhir_aggregator.config import Config, load_default_config

try:
    config = load_default_config()
except Exception as e:
    logging.error(f"Error loading config : {e}")
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
    
    1.  Triggered when the `ingest_data_dag-raw` dataset is updated.
    2. Reads the manifest of ingested files from the upstream DAG's inlet dataset.
    3.  Iterates through the ingested files.
    4.  Performs transformations on each file (currently a placeholder).
    5.  Uploads the transformed files to the specified GCS bucket.
    6.  Creates a manifest of transformed files.
    7.  Updates the Airflow outlet dataset with the manifest of transformed files.
    
    
    **Considerations:**
    
    *   The placeholder transformation needs to be replaced with your actual transformation logic.
    *   Error handling and logging should be enhanced for production use.
    *   The input directory is hardcoded as `/tmp/data`.  Change as necessary.
    """,
    )
    def transform_dag(**kwargs):

        inlet_dataset = Dataset(f"{prefix}-raw")
        outlet_dataset = Dataset(f"{prefix}-processed")

        def _transform(processed_file_names: list[str]) -> list[str]:
            """Transforms data and creates a manifest of transformed files."""
            try:
                transformed_filenames = []
                for _ in processed_file_names:
                    transformed_filename = _.replace(f"{prefix}/", f"{prefix}/R4/")
                    transformed_filenames.append(transformed_filename)
                return transformed_filenames

                # storage_client = storage.Client()
                # output_bucket = storage_client.bucket(config.output_bucket)
                # transformed_files = []
                # input_dir = "/tmp/data"
                #
                # for file_data in manifest["files"]:
                #     filepath = os.path.join(input_dir, file_data["name"])
                #     if os.path.exists(filepath):
                #         # Placeholder transformation. Replace this!
                #         transformed_filename = file_data["name"].replace(
                #             f"{config.prefix}/", f"{config.prefix}/R4/"
                #         )
                #         blob = output_bucket.blob(transformed_filename)
                #         # blob.upload_from_string(...)  # Upload transformed data here
                #         transformed_files.append(transformed_filename)
                # return {"manifest": transformed_files}
            except Exception as e:
                print(f"Error transforming data: {e}")
                return []

        @task(inlets=[inlet_dataset], outlets=[outlet_dataset])
        def transform(inlet_events, outlet_events):
            """Reads the manifest from the inlet dataset."""
            events = inlet_events[inlet_dataset]
            assert len(events) > 0, "Should have at least 1 event"
            inlet_event = events[-1]
            assert (
                "manifest" in inlet_event.extra
            ), f"manifest not found in inlet_event {inlet_event.extra}"
            transformed_manifest = _transform(inlet_event.extra["manifest"])
            outlet_events[outlet_dataset].extra = {"manifest": transformed_manifest}
            return transformed_manifest

        transform()

    dags.append(transform_dag())

dags
