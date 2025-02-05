Here is a `README.md` file for the `mkdag` project:

```markdown
# mkdag

`mkdag` is a CLI tool to render Apache Airflow DAGs from YAML configuration files using Jinja templates. It simplifies the process of generating DAGs and hooks by providing a structured approach to configuration and rendering.

## Features

- Generate Airflow DAGs from YAML configuration files
- Generate Airflow Hooks from YAML configuration files
- Automatically format generated Python code using `black`
- Validate generated Python code for syntax errors

## Installation

To install `mkdag`, clone the repository and install the required dependencies:

```sh
git clone git@github.com:bmeg/tractor.git
cd tractor
cd mkdag
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Command Line Interface

The `mkdag` CLI provides commands to render DAGs and hooks from YAML configuration files.

#### Render DAG

To render a DAG from a YAML configuration file, use the `render-dag` command:

```sh
python -m mkdag.cli render-dag --config path/to/config.yaml --output path/to/output
```

- `--config` (`-c`): Path to the YAML configuration file.
- `--output` (`-o`): Directory for the generated DAG files. Default is the current directory.

### Example Configuration

Here is an example of a YAML configuration file:

```yaml
dag:
  id: simple_gene_processing
  description: A simple DAG using task decorators
  schedule_interval: "@daily"
  start_date: "2025-01-01"
  default_args:
    retries: 3
    retry_delay: 5
  # optional, if not provided, default ETL workflow will be used
  tasks:
    - id: check
      type: sensor
      poke_interval: 60
      timeout: 120
      mode: poke
      soft_fail: true
    - id: extract
      type: task
    - id: transform
      type: task
    - id: validate
      type: task
    - id: load
      type: task
extra:
  configuration:
    inputs:
      input1: "s3://path/to/input1"
    outputs:
      output1: "s3://path/to/output1"
```

### Running Tests

To run the tests, use the following command:

```sh
pytest
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
