import pathlib
import subprocess

import click
import yaml
from jinja2 import Environment, FileSystemLoader

from mkdag.dag_generator import DAGGenerator, HookGenerator, ConfigGenerator

DEFAULT_OUTPUT_PATH = "."


@click.command(name="render-dag")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    required=True,
    help="Path to the YAML configuration file.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=True),
    default=DEFAULT_OUTPUT_PATH,
    help=f"Directory for the generated DAG files [dags/, plugins/hooks, config/]. Default is {DEFAULT_OUTPUT_PATH}",
)
def render_dag_cli(config, output):
    """CLI tool to render an Airflow DAG from a YAML configuration file using Jinja."""
    try:

        assert config.endswith('.yaml') or config.endswith('.yml'), 'config file must be a YAML file'
        config = yaml.load(open(config), yaml.SafeLoader)

        output = pathlib.Path(output)
        dag_output = output / "dags"
        hook_output = output / "plugins/hooks"
        config_output = output / "config"
        dag_output.mkdir(parents=True, exist_ok=True)
        hook_output.mkdir(parents=True, exist_ok=True)
        config_output.mkdir(parents=True, exist_ok=True)

        fp = render_dag(config, dag_output)
        click.echo(f"✅ DAG file successfully generated: {fp}")

        fp = render_hook(config, hook_output)
        click.echo(f"✅ Hook file successfully generated: {fp}")

        fp = render_config(config, config_output)
        click.echo(f"✅ Config file successfully generated: {fp}")

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        exit(1)


def render_dag(config: dict, output_path: str) -> pathlib.Path:
    """Render an Airflow DAG from a YAML configuration file."""

    generator = DAGGenerator(config)
    dag = generator.render()
    # Save the rendered DAG
    file_path = pathlib.Path(output_path) / f"{generator.id}_dag.py"
    with open(file_path, "w") as dag_file:
        dag_file.write(dag)
    reformat_with_black(file_path)
    return file_path


def render_hook(config: dict, output_path: str) -> pathlib.Path:
    """Render an Airflow Hook from a YAML configuration file."""

    generator = HookGenerator(config)
    hooks = generator.render()
    # Save the rendered Hook
    file_path = pathlib.Path(output_path) / f"{generator.id}_hook.py"
    with open(file_path, "w") as f:
        f.write(hooks)
    reformat_with_black(file_path)
    return file_path


def render_config(config: dict, output_path: str) -> pathlib.Path:
    """Render an Airflow Hook from a YAML configuration file."""

    generator = ConfigGenerator(config)
    etl_config = generator.render()
    # Save the rendered config
    file_path = pathlib.Path(output_path) / f"{generator.id}_config.json"
    with open(file_path, "w") as f:
        f.write(etl_config)
    return file_path


def reformat_with_black(output_file):
    """Reformat the output file using black."""
    black_result = subprocess.run(['black', output_file], capture_output=True, text=True)
    if black_result.returncode != 0:
        raise ValueError(f"Black formatting failed: {black_result.stdout} {black_result.stderr}")


if __name__ == "__main__":
    render_dag_cli()
