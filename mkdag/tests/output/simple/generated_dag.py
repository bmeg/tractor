from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime.strptime("2025-01-01", "%Y-%m-%d"),
}

with DAG("test_dag", default_args=default_args, schedule_interval="@daily") as dag:
    start = DummyOperator(task_id="start")
