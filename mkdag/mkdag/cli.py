import subprocess

import click
import yaml
from jinja2 import Environment, FileSystemLoader


@click.command(name="render-dag")
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    required=True,
    help="Path to the YAML configuration file.",
)
@click.option(
    "--template",
    "-t",
    type=click.Path(exists=True),
    required=True,
    help="Path to the Jinja template file.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default="generated_dag.py",
    help="Path where the generated DAG file should be saved (default: generated_dag.py).",
)
def render_dag_cli(config, template, output):
    """CLI tool to render an Airflow DAG from a YAML configuration file using Jinja."""
    try:
        render_dag(config, output, template)

        click.echo(f"✅ DAG file successfully generated: {output}")

    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        exit(1)


def render_dag(config, output, template):
    """Render an Airflow DAG from a YAML configuration file using Jinja."""
    # Load YAML config
    with open(config, "r") as file:
        config_data = yaml.safe_load(file)
    assert 'dag_id' in config_data, 'dag_id not found in config file'
    # Load Jinja template
    env = Environment(loader=FileSystemLoader("."), autoescape=True)
    template_obj = env.get_template(template)
    # Render template
    dag_code = template_obj.render(config_data)
    # Save the rendered DAG
    with open(output, "w") as dag_file:
        dag_file.write(dag_code)
    if output.endswith('.py'):
        reformat_with_black(output)


def reformat_with_black(output_file):
    """Reformat the output file using black."""
    black_result = subprocess.run(['black', output_file], capture_output=True, text=True)
    if black_result.returncode == 0:
        print(f"Black formatting applied successfully to {output_file}")
    else:
        print(f"Black formatting failed: {black_result.stdout} {black_result.stderr}")


if __name__ == "__main__":
    render_dag_cli()
