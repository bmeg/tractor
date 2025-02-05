import json
import yaml
import click


@click.command()
@click.argument('input_path', type=click.Path(exists=True))
@click.argument('output_path', default=None, required=False)
def json_to_yaml(input_path, output_path):
    """Convert JSON file to YAML file."""
    with open(input_path, 'r') as json_file:
        dag_json = json.load(json_file)

    if not output_path:
        output_path = input_path.replace('.json', '.yaml')
    with open(output_path, 'w') as yaml_file:
        yaml.dump(dag_json, yaml_file, default_flow_style=False)

    click.echo(f"Converted {input_path} to {output_path}")


if __name__ == '__main__':
    json_to_yaml()
