from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Connection
from airflow.hooks.base import BaseHook
import tes

import logging
logger = logging.getLogger(__name__)


def get_tes_connection(connection_id="TES") -> Connection:
    """Get the TES connection from airflow."""
    return BaseHook.get_connection(connection_id)


class TESOperator(BaseOperator):
    """Implement a Task Execution Service (TES) operator."""
    tes_task: tes.Task

    def __init__(self, tes_task, *args, **kwargs):
        """Initialize the TESOperator with a TES task."""
        self.tes_task = tes_task
        self.airflow_connection = None
        super().__init__(*args, **kwargs)

    def execute(self, context) -> Any:
        """Execute the TES task."""
        # Create client
        # TODO - how to pass user credentials?, they are in the connection, but no way to supply to the tes client
        # Create client
        if not self.airflow_connection:
            self.airflow_connection = get_tes_connection()
        logger.info(self.airflow_connection.as_json())
        url = f"{self.airflow_connection.schema}://{self.airflow_connection.host}:{self.airflow_connection.port}"
        client = tes.HTTPClient(url, timeout=5)
        task_id = client.create_task(self.tes_task)
        logger.warning(f"Task ID: {task_id}")
        # Create and run task
        # TODO - how to configure wait?, should we have a sensor? ie have one airflow task to start tes.Task and another to wait "sense" for it to complete
        client.wait(task_id, timeout=0)  # wait for task to complete
        task = client.get_task(task_id, view="BASIC")
        if task.state == "COMPLETE":
            return task.outputs
        raise AirflowException(f"Task failed: {task.state} {task}")


# print("FOOOO")
# with DAG(dag_id="tes_hello_world", start_date=datetime(2021, 1, 1)) as dag:
#     # Create a TES task
#     task = tes.Task(
#         name="Hello World",
#         executors=[tes.Executor(image="alpine", command=["echo", "hello world"])],
#     )
#     # Create a TES operator
#     operator = TESOperator(task_id="hello_world", dag=dag, tes_task=task)
#
#     operator
# dag