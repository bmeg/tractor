from collections import defaultdict

import yaml


def get_search_paths(yaml_file: str) -> dict:
    """
    Read the YAML file and return all paths that end in 'search' into a dictionary with key 'tag'.

    :param yaml_file: Path to the YAML file.
    :return: Dictionary containing paths that end in 'search' with key 'tag'.
    """
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)

    search_paths = {key: value for key, value in data['paths'].items() if key.endswith('search')}

    entities = defaultdict(list)

    for key, value in search_paths.items():
        entities[value['get']['tags'][0]].append(key)
    return dict(entities)


# Example usage
yaml_file = 'stapi.yaml'
search_paths = get_search_paths(yaml_file)
for k, v in search_paths.items():

    config = {
        "dag_id": f"stapi-{k}",
        "description": f"A Star Trek API DAG for {k}",
        "inputs": {
            f"{k}.json": f"/api{v[-1]}"
        },
        "http_conn_id": "stapi-http",
        "aws_conn_id": "stapi-aws",
        "tags": ["demo"]
    }
    with open(f"stapi-{k}.yaml", 'w') as file:
        file.write(yaml.dump(config, default_flow_style=False))
