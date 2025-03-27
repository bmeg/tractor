# create_fhir_aggregator

## Overview
create_fhir_aggregator.py is a Python script designed to create Directed Acyclic Graphs (DAGs) for running in an Apache Airflow environment. This script automates the generation of DAGs based on predefined templates and configurations, facilitating the setup and execution of workflows in Airflow.

## Features
* Generates DAGs for Airflow.
* Supports customizable templates and configurations.
* Automates the setup of complex workflows.


## Usage
* Place the `tractor` directory in the root of your Airflow installation.
* Place the `create_fhir_aggregator.py` script in the root of `dags` directory of your Airflow installation. Run the script to generate the DAGs based on the specified templates and configurations.
* Prompt airflow to refresh the DAGs by running:
```
dc exec -it  airflow-worker airflow  dags reserialize -S /opt/airflow/dags/create_fhir_aggregator.py
```

## Customization
See `dags/tractor/etl_dsl/generators/fhir_aggregator.py` for configuration options.