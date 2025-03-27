import requests
import json

# Airflow REST API endpoint
airflow_url = "http://localhost:8080/api/v1"

# Airflow authentication (if needed)
auth = ("airflow", "airflow")

# List all datasets
response = requests.get(f"{airflow_url}/datasets?uri_pattern=stapi", auth=auth)
response_dict: dict = response.json()
datasets = response_dict['datasets']

# Delete each dataset
for dataset in datasets:
    dataset_id = dataset['id']
    print(f"Deleting dataset: {dataset_id}")
    delete_response = requests.delete(f"{airflow_url}/datasets/{dataset_id}", auth=auth)
    if delete_response.status_code == 204:
        print(f"Deleted dataset: {dataset_id}")
    else:
        print(f"Failed to delete dataset: {dataset_id} {delete_response}")

print("All datasets have been deleted.")
