import logging
from typing import Any

import tes
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Connection

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
        logger.info(self.tes_task.as_json())
        url = f"{self.airflow_connection.schema}://{self.airflow_connection.host}:{self.airflow_connection.port}"
        step_error_message = "Creating TES task"
        try:
            client = tes.HTTPClient(url, timeout=5)
            task_id = client.create_task(self.tes_task)
            logger.info(f"Task ID: {task_id}")
            # Create and run task
            step_error_message = f"Waiting on TES task to complete task_id: {task_id}"
            # TODO - how to configure wait?, should we have a sensor? ie have one airflow task to start tes.Task and another to wait "sense" for it to complete
            client.wait(task_id, timeout=0)  # wait for task to complete
            step_error_message = f"Getting TES task view FULL task_id: {task_id}"
            task = client.get_task(task_id, view="FULL")
            if task.state == "COMPLETE":
                return task.outputs
            raise AirflowException(
                f"Task failed: {step_error_message} {task.state} {task}"
            )
        except Exception as e:
            _ = f"TES Error: {step_error_message} {e}"
            logger.error(_)
            raise AirflowException(_)
