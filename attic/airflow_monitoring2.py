"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import sys

import airflow
from airflow import DAG
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'airflow_monitoring2',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='*/10 * * * *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)


def print_python_version():
    print(f"Python version: {sys.version}")


# t0 = PythonOperator(
#     task_id='print_python_version',
#     python_callable=print_python_version,
#     dag=dag)

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='python-version',
    bash_command='python --version',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
    do_xcom_push=False)



t1

# t0 >> t1
# END
