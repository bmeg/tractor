# STAPI DAG Generator

This script is responsible for generating Airflow DAGs for the Star Trek API (STAPI) project. It uses the `generate` function from the `tractor.mkdag` module and yaml files in the `projects/stapi` directory to create DAGs based on YAML configuration files.
**Caution** - Apache Airflow re-creates dynamic DAGs when the source code changes by continuously monitoring the DAG files in the `dags_folder`. When a change is detected, Airflow parses the updated DAG files and dynamically generates the DAGs based on the new code. This allows for flexible and dynamic workflows that can adapt to changes in the source code without requiring a restart of the Airflow scheduler. For more information, refer to the [Airflow DAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html) and [Dynamic DAGs](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#dynamic-dags) documentation.  This means that if there is any changes or errors in the DAG generation, it will impact existing workflows.  Having a separate script like this **per project** will help isolate unintended changes.

```commandline
#  for example, to reserialize only the STAPI DAGs 
#   -S, --subdir SUBDIR   File location or directory from which to look for the dag.
docker compose exec -it airflow-worker airflow dags reserialize -S /opt/airflow/dags/stapi
```
**Note** - The actual implementation of the `generate` function is not provided in this documentation, as it is part of the `tractor.mkdag` module.

## Files

- `stapi/generate.py`: Main script for generating STAPI DAGs.

## Modules and Functions

### `generate.py`


## Usage

To run the script, simply execute it in an environment where Airflow is configured and the `tractor.mkdag` module is available. Ensure that the YAML configuration files are located in the `/opt/airflow/projects/stapi` directory.

## STAPI Binner DAGs

This script defines Airflow DAGs for binning datasets from the Star Trek API (STAPI) project. The datasets are divided into four bins based on the first character after the "stapi-" prefix and processed accordingly.
This is an example of a larger dataset or project that requires binning to manage the processing of data in smaller chunks. This script demonstrates how to divide the datasets into bins and create separate DAGs for processing each bin.


## Files

- `stapi-binner-dag.py`: Main script for defining and managing the binner DAGs.

## Modules and Functions

### `stapi-binner-dag.py`
#### Code

The script performs the following steps:

1. **Define Dataset Names**: Lists all dataset names to be processed.  TODO this could be a list of datasets from querying airflow and matching dataset name convention.


2. **Divide Datasets into Bins**: Divides the datasets into four bins based on the first character after the "stapi-" prefix.. 
    ```python
    bins = {
        "A-C": [name for name in dataset_names if "A" <= get_first_char(name) <= "C"],
        "D-L": [name for name in dataset_names if "D" <= get_first_char(name) <= "L"],
        "M-S": [name for name in dataset_names if "M" <= get_first_char(name) <= "S"],
        "T-Z": [name for name in dataset_names if "T" <= get_first_char(name) <= "Z"],
    }
    ```

3. **Define DAGs for Each Bin**: Defines individual DAGs for each bin (A-C, D-L, M-S, T-Z).
    ```python
    @dag(...)
    def bin_dag_a_c():
        ...
    bin_dag_a_c()
    bin_dag_d_l()
    bin_dag_m_s()
    bin_dag_t_z()
    ```

4. **Define Loader DAG**: Defines a loader DAG to process the combined datasets.
    ```python
    @dag(...)
    def loader():
        ...
    ```

![Image](https://github.com/user-attachments/assets/28b5c90c-3529-43db-b513-20d9345a5a4b)
