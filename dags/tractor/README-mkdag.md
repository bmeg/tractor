# mkdag

This directory contains the Python code for generating and managing Airflow DAGs for the Tractor project. The code is organized into several modules and classes, each responsible for different aspects of the DAGs.

## Files

- `mkdag.py`: Main script for generating dynamic ETL DAGs based on a YAML configuration file.

## Modules and Classes

### `mkdag.py`


#### Classes

##### `Config`

### Minimal Inputs for Configuring an ETL
 
- **`dag_id` (str)**: The unique identifier for the DAG.
- **`description` (str)**: A brief description of the DAG.
- **`inputs` (dict[str, str])**: A dictionary of input sources. Typically, these are named URLs or file paths.

The `Config.check_config` method ensures that all required attributes are set correctly and fills in default values where necessary. Based on this, the minimal inputs required for configuring an ETL are as follows:

    1. Ensure `dag_id` is provided.
    2. Ensure at least one input is provided.
    3. Set default values for `aws_conn_id`, `bucket_name`, `s3_prefix`, and `http_conn_id` if not provided.
    4. Convert `schedule_dataset`, `raw_dataset`, and `processed_dataset` to `Dataset` objects if they are strings.
    5. Set default `response_check` if not provided, using the first input URL.
    6. Set default `checker` and `method` for `response_check` based on its type.
    7. Append `dag_id` and "dynamic" to the tags.
    8. Update the description with the current timestamp.
    9. Set default `start_date` to one day ago if not provided.

### Abstracting Configuration 

Keeping configuration details, passwords, and other sensitive information in the Airflow secrets configuration provides several benefits:

1. **Security**: Encrypt and control access to sensitive information using a secure secrets backend (e.g., AWS Secrets Manager, HashiCorp Vault, Azure Key Vault).

2. **Centralized Management**: Manage and rotate credentials centrally without modifying DAG code, simplifying management across environments.

3. **Separation of Concerns**: Keep configuration details and secrets separate from DAG code for better readability and maintainability.

4. **Compliance**: Use a secrets management system to comply with security standards and regulations.

5. **Environment-Specific Configurations**: Configure secrets differently for various environments (development, staging, production) without changing DAG code.

For more information, refer to the [Airflow documentation on Secrets Backends](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html).


#### Functions


##### `read_config(config_path: str)`

Reads a YAML configuration file.

##### `generate_extract_dag(config: Config) -> DAG`

Generates a DAG for extracting data from a source.

##### `generate_transform_dag(config: Config)`

Generates a DAG for transforming data.

##### `generate_load_dag(config: Config)`

Generates a DAG for loading data.

##### `generate_dags_from_config(config_path: str) -> list[DAG]`

Generates a list of DAGs from a YAML configuration file.

##### `generate(config_file_path: str = None) -> list[DAG]`

Generates a list of DAG from all yaml files in a directory.

## Usage

To generate the DAGs, call the `generate` function with the path to the YAML configuration file:

```python
from dags.tractor.mkdag import generate

dags = generate("path/to/configs")