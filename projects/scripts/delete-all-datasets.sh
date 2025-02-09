#!/bin/bash

# List all datasets
datasets=$(airflow datasets list --output json | jq -r '.[].dataset_id')

# Delete each dataset
for dataset in $datasets; do
    echo "Deleting dataset: $dataset"
    # airflow datasets delete "$dataset"
done

echo "All datasets have been deleted."
