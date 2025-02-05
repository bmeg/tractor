

import os
import tractor
from datetime import datetime
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

import logging

logger = logging.getLogger(__name__)

PROJECT_DIR = Variable.get("project_dir")
logger.info(f"Tractor-Project {PROJECT_DIR} exists: {os.path.exists(PROJECT_DIR)}")


if os.path.exists(PROJECT_DIR):
    with DAG(dag_id="tractor-build", schedule=None, start_date=datetime(2022, 3, 4)) as dag:
        logging.info("Building Tractor DAG")
        tractor.build(PROJECT_DIR, dag)