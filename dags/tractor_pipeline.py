

import os
import tractor
from datetime import datetime
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

import logging

logger = logging.getLogger(__name__)

PROJECT_DIR = Variable.get("project_dir", default_var=None)
if PROJECT_DIR and os.path.exists(PROJECT_DIR):
    logger.info(f"Tractor-Project {PROJECT_DIR}")
else:
    logger.warning(f"Tractor-Project variable `project_dir`={PROJECT_DIR} not found or path does not exist")
    exit(0)

with DAG(dag_id="tractor-build", schedule=None, start_date=datetime(2022, 3, 4)) as dag:
    tractor.build(PROJECT_DIR, dag)
