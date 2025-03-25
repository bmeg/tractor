# Airflow Data Pipeline for Indonesian Food Recipes

This repository contains Airflow DAGs for an ETL pipeline processing Indonesian food recipe data.  The pipeline extracts data from various sources, transforms it, and loads it into BigQuery.

## Pipeline Overview

The pipeline consists of three main DAGs:

1.  **`ingest_data_dag`:** Extracts data from specified sources (currently configured for Google Cloud Storage) and creates a manifest of ingested files, storing the manifest in the dataset's extra field.
2.  **`transform_data_dag`:** Reads the ingested data, performs transformations (detailed below), and writes transformed data as NDJSON files to Google Cloud Storage, creating a new manifest of processed files in the dataset's extra field.
3.  **`load_fhir_dag`:** Loads the transformed NDJSON data (assuming FHIR JSON format) into a Google Healthcare FHIR service.

## Data Sources and Transformations

Currently, the pipeline is configured to read data from Google Cloud Storage.  The `transform_data` task in `transform_data_dag.py` is a placeholder and needs to be customized to reflect your actual data transformation logic.  The transformations should convert the data into a format suitable for loading into the FHIR service, specifically, the data needs to be in NDJSON format.


## Configuration

The pipeline's behavior is controlled by a `config.json` file.  This file should contain the following:

*   `"bucket"`: The name of the Google Cloud Storage bucket containing the input data.
*   `"expected_files"`: A list of expected file names in the input bucket.
*   `"dag_id_prefix"`: A prefix to add to the DAG IDs for easy identification and management of multiple pipelines.
*   `"output_bucket"`: The Google Cloud Storage bucket for storing transformed data.
*   `"project_id"`: Your Google Cloud Project ID.
*   `"location_id"`: The location of your Google Cloud project (e.g., "us-central1").
*   `"fhir_store_id"`: The ID of your Google Healthcare FHIR store.


## Setup

1.  **Install Airflow:** Follow instructions for your preferred method (local or Docker).
2.  **Install Providers:** Ensure you have the required Airflow providers installed (`apache-airflow[google,google_cloud_bigquery,amazon,...]`).
3.  **Configure Google Cloud:** Set up authentication for Google Cloud services (service accounts are recommended).
4.  **Create Buckets:** Create the input and output Google Cloud Storage buckets.
5.  **Create FHIR Store:**  Set up a FHIR store in Google Healthcare.
6.  **Configure `config.json`:** Fill in the configuration values in `config.json`.
7.  **Run DAGs:** Upload the DAG files to your Airflow environment and trigger them manually in the order of `ingest_data_dag`, `transform_data_dag`, and finally `load_fhir_dag`.

```
# if on laptop?
airflow connections add 'fhir-agg-gcp' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{"extra__google_cloud_platform__key_path": "/path/to/your/service-account-file.json", "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform"}'
    --conn-description 'Google Cloud Platform connection for FHIR Aggregator'
```

## Notes

*   Error handling and logging are crucial for production environments.  The provided code includes basic error handling, but it should be improved for real-world use.
*   The transformation step is a placeholder.  Implement your specific transformations in the `transform_data` function.
*   Thoroughly test each DAG separately before integrating them into a complete pipeline.
*   The DAGs assume the existence of the necessary BigQuery datasets. Create these before running the DAGs.


## Future Improvements

*   Add more robust error handling and logging.
*   Implement more sophisticated data transformation logic.
*   Add data quality checks.
*   Integrate with a data monitoring tool.


This README provides a high-level overview. Refer to the individual DAG files for more detailed information.

