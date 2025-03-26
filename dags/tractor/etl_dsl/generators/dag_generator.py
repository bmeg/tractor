import importlib
import logging
import os
from copy import deepcopy
from datetime import datetime
from typing import Callable

from airflow import DAG, Dataset
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pydantic import BaseModel

from tractor.etl_dsl.model import ETLProject, Task, CommandDetailed
from tractor.tes_operator import TESOperator

log = logging.getLogger(__name__)

LOGGED_ALREADY = []


class AggregateLoadDagGenerator(BaseModel):
    """Generates a set of DAGs with a DAG per source. see https://airflow.apache.org/docs/apache-airflow/2.5.0/concepts/dags.html#taskgroups"""

    project: ETLProject

    def mkdags(self) -> list[DAG]:
        """Generate mock DAGs for the project.
        :return:
        """
        # Create a list of DAGs
        assert len(self.project.sources) > 0, "Expected sources to be created"
        project = self.project
        dags = []

        for source in project.sources:
            for etl_verb in ["extractor", "transformer", "loader"]:
                task = getattr(source, etl_verb)
                if task:
                    task_id = f"{project.id}-{source.id}-{etl_verb}"
                    dag_id = task_id
                    tags = [
                        etl_verb,
                        project.id,
                    ]
                    outlets = [Dataset(**_.model_dump()) for _ in task.outlets or []]
                    inlets = [Dataset(**_.model_dump()) for _ in task.inlets or []]
                    dags.append(
                        self.create_dag(
                            dag_id=dag_id,
                            task_id=task_id,
                            tags=tags,
                            inlets=inlets,
                            outlets=outlets,
                            task=task,
                        )
                    )

        if project.loader:
            task_id = f"{project.id}-loader"
            dag_id = f"{project.id}-loader"
            task = project.loader
            tags = [
                "load",
                project.id,
            ]

            outlets = [Dataset(**_.model_dump()) for _ in task.outlets]
            inlets = [Dataset(**_.model_dump()) for _ in task.inlets]

            dags.append(
                self.create_dag(
                    dag_id=dag_id,
                    task_id=task_id,
                    tags=tags,
                    inlets=inlets,
                    outlets=outlets,
                    task=task,
                )
            )

        return dags

    def create_dag(
        self,
        dag_id: str,
        task_id: str,
        tags: list[str],
        inlets: list[Dataset],
        outlets: list[Dataset],
        task: Task,
    ) -> DAG:
        schedule = inlets or None

        with DAG(
            dag_id=dag_id,
            default_args={
                "owner": "airflow",
            },
            schedule=schedule,
            start_date=datetime(2024, 2, 1),
            tags=tags,
            is_paused_upon_creation=True,
        ) as dag:
            parms = {
                "retries": 0,
                "task_id": task_id,
                "inlets": inlets,
                "outlets": outlets,
            }
            operator = self.ensure_operator(parms, task=task)
            operator.dag = dag
            return dag

    def ensure_operator(self, operator_parms: dict, task: Task) -> BaseOperator:
        """Ensure the operator is created, uses the TESOperator as default based on image is not known.

        Args:
            operator_parms: dictionary of parameters for the operator
            task: task
        """
        project = self.project  # noqa

        original_operator_parms = deepcopy(operator_parms)
        original_task_id = original_operator_parms["task_id"]

        _operator_parms = deepcopy(original_operator_parms)
        assert isinstance(
            task.command, CommandDetailed
        ), f"Expected CommandDetailed got {task.command} see project.apply_defaults()"

        # add callbacks on all
        if task.callbacks:
            if task.callbacks.get("pre_execute"):
                _operator_parms["pre_execute"] = ensure_python_callable(
                    task.callbacks.get("pre_execute")
                )
            if task.callbacks.get("on_success"):
                _operator_parms["on_success_callback"] = ensure_python_callable(
                    task.callbacks.get("on_success")
                )
            if task.callbacks.get("on_failure"):
                _operator_parms["on_failure_callback"] = ensure_python_callable(
                    task.callbacks.get("on_failure")
                )
        operator: BaseOperator
        if task.command.operator_type == "BashOperator":
            _operator_parms["cwd"] = make_tmp_dir(original_task_id)
            operator = BashOperator(
                **_operator_parms, bash_command=task.command.command
            )
        elif task.command.operator_type == "PythonOperator":
            operator = PythonOperator(
                **_operator_parms,
                python_callable=ensure_python_callable(task.command.command),
            )
        else:
            operator = TESOperator(**_operator_parms, tes_task=task.to_tes())

        return operator


def make_tmp_dir(dag_id: str) -> str:
    """
    Create a temporary directory, /tmp + DAG ID using the convention of project-source-[ETL] e.g. /tmp/project-source.

    Args:
        dag_id (str): The ID of the DAG.

    Returns:
        str: The path to the temporary directory.
    """
    dag_id_parts = dag_id.split("-")
    tmp_dir = "/tmp/" + "-".join(dag_id_parts[:-1])
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir


def ensure_python_callable(command: str) -> Callable:
    """Split the command to get module and function names"""
    if "." not in command:
        msg = f"ensure_python_callable command: {command} missing module name, using builtins"
        if msg not in LOGGED_ALREADY:
            LOGGED_ALREADY.append(msg)
            log.warning(msg)
        module_name = "builtins"
        function_name = command
    else:
        module_name, function_name = command.rsplit(".", 1)
    # Import the module
    module = importlib.import_module(module_name)
    # Get the function from the module
    return getattr(module, function_name)
