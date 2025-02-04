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
print(search_paths)