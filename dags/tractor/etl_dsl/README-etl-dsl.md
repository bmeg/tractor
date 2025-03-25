From: dags/tractor/etl_dsl/README-etl-dsl.md

# ETL DSL Documentation

## Overview

This document describes the Domain-Specific Language (DSL) used for defining Extract, Transform, Load (ETL) processes. The DSL allows users to specify data sources, extraction commands, transformation steps, and loading procedures in a structured and readable format.

By using YAML format, it ensures readability and ease of use, allowing users to quickly set up and manage their ETL workflows. It is important to note that this DSL is not a general graph definition language, but a specific set of shorthand configurations tailored for defining ETL processes. This specialization allows for more concise and focused definitions, making it easier to manage ETL tasks and their dependencies.

## Structure ETL DSL Schema

This repository contains a JSON schema for defining a Domain-Specific Language (DSL) for ETL (Extract, Transform, Load) projects. The schema provides a structured way to define ETL processes, including commands, defaults, sources, and storage objects.

### Schema Overview

The schema is defined using JSON Schema Draft-07 and includes the following main components:

- **ETLProject**
![image](https://github.com/user-attachments/assets/7235075f-4f52-4f7e-8104-4be6be0f55b7)

- **Source**: Represents a data source with properties for commands, inputs, and outputs.
![image](https://github.com/user-attachments/assets/fce4c200-d840-4870-b801-f0e13c2539f8)

- **Task/Command**: Represents a command to be executed, with inputs &/or outputs.  The command, input and output can be specified as simple strings, or as a detailed object
![image](https://github.com/user-attachments/assets/f5923533-ae80-4713-8388-544ca7fa659b)
![image](https://github.com/user-attachments/assets/96e0fc81-76c7-4230-a437-d4ac78eab5fd)

- **StorageObject/Dataset**: The StorageObject represents an input or output storage object, which can be a string or an object with properties like `url`, `path`, `type`, `name`, and `description`.  This is intended as a parameter i.e. the task's inputs or outputs.   The Dataset represents an event, used to trigger downstream processing i.e. the inlets are the datasets the task will consume, the outlets are the datasets the task will produce.  
Each inputs and outputs are associated with a `Dataset`
Airflow Datasets can be scheduled based on the dependencies defined in the ETL process. The scheduling ensures that tasks are executed in the correct order, respecting the dependencies between different datasets. 

![image](https://github.com/user-attachments/assets/3b677a69-c296-4494-b082-5b6830bb342d)


- **Defaults**: Defines default values for the ETL project, including bucket information, operator type, and default commands.
![image](https://github.com/user-attachments/assets/410eea2c-28de-4621-a84d-877022cce613)


- **ETLProject**: All of these entities are combined in an ETLProject.  It is the key integration point for serialization and DAG generation.
![image](https://github.com/user-attachments/assets/70daa2dd-95b0-42ab-907f-8c522fb697d3)



### References

- [JSON Schema](http://json-schema.org/)
- [Apache Airflow Operators](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html)
- [AWS Connection](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html)
- [GCP Connection](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)

### Example

Here is an example of an ETL `Process` defined using the DSL:

```yaml
---
# This globally unique id is used to identify the ETL Project in the console and logs
id: example_etl

# these defaults are used to expand and fill in missing values in the sources section
defaults:
  # e.g. all data will be stored in the bucket named 'ExampleBucket'
  bucket: ExampleBucket
  bucket_prefix: example
  bucket_scheme: s3
  # e.g. all commands will be executed using the BashOperator
  operator_type: BashOperator
  extractor:
    command: echo "extract"
  transformer:
    command: echo "transform"

sources:
  - id: source1
    # this source overrides the default extract commands
    extractor:
      command: echo "extract source1"
      # this source uses the defaults.transform_commands
      outputs:
          # specific files are identified as outputs
          # on successful completion of the extract and transform commands, these files will be uploaded to the bucket
          - source1/raw/data.json
          - source1/raw/lookup.tsv
          - source1/processed/combined.ndjson

  - id: source2
    # this source overrides the default extract and transform commands
    extractor:
      command: wget https://example.com/data.csv -O source2/raw/data.csv
      outputs:
        # directories are used as outputs
        - source2/raw
    transformer:
      command: python3 source2/src/process.py
      outputs:
        - source2/processed

  - id: source3
    # this source uses the default extract and transform commands
    loader:
      # this source has a separate load command that will be scheduled after its transform
      command: echo "load-source3-sidecar"
      inputs:
        # directories are used as inputs
        # the contents of these directories will be downloaded from the bucket and made available to the extract and transform commands
        - source3/external-data

```


## Resulting DAGs

The ETL process defined using this DSL will be translated into a Directed Acyclic Graph (DAG) by the ETL engine. The DAG will represent the workflow of the ETL process, with nodes corresponding to tasks and edges representing dependencies between tasks. The DAG can be visualized and executed by the ETL engine to perform the specified ETL operations.

Source DAG Creation:  Each Source in the ETL process creates a Directed Acyclic Graph (DAG) with the following characteristics:
* Extract: The extract step is triggered by either a Console operation, CLI command, API call, or scheduled similar to cron.
* Transform: The transform step is triggered when the extract step updates its Dataset.
* Load: The load step is triggered when the transform step updates its Dataset.

Process DAG Creation: The Process DAG is created by combining the DAGs of all the Sources in the ETL process. The Process DAG has the following characteristics:
* Load: The load step is triggered when all the Sources have completed their Load steps.

Example:
![image](https://github.com/user-attachments/assets/b74f9a35-e626-4545-86f6-ebe6a6894f4d)

## Extensions

See `tractor/etl_dsl/generators/fhir_aggregator.py` and `tractor/etl_dsl/generators/grip.py` They extend etl_dsl.model with use case specific properties

## DAG generation

See `tractor/etl_dsl/generators/dag_generator.py` this class is coupled with the model and will generate the airflow DAGs.   The files `fhir_aggregator.py` and `grip.py` associate the model and dag_generator with a specific configuration.

## Airflow integration

See `dags/create_fhir_aggregator.py` and `dags/create_grip.py` The airflow scheduler will find and parse the resulting DAGs.


## Conclusion

This DSL provides a simple and structured way to define ETL processes. By using YAML format, it ensures readability and ease of use, allowing users to quickly set up and manage their ETL workflows.