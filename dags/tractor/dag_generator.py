import importlib
import logging
from copy import deepcopy
from datetime import datetime
from typing import Protocol

import tes
from airflow import DAG, Dataset
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from tractor.config import Project, DagConfig
from tractor.tes_operator import TESOperator

log = logging.getLogger(__name__)

LOGGED_ALREADY = []


def ensure_python_callable(command: str) -> callable:
    """Split the command to get module and function names"""
    if "." not in command:
        msg = f"ensure_python_callable command: {command} missing module name, using builtins"
        if msg not in LOGGED_ALREADY:
            LOGGED_ALREADY.append(msg)
            log.warning(msg)
        module_name = "builtins"
        function_name = command
    else:
        module_name, function_name = command.rsplit('.', 1)
    # Import the module
    module = importlib.import_module(module_name)
    # Get the function from the module
    return getattr(module, function_name)


class BaseDAGGenerator(Protocol):
    """A protocol for generating DAGs."""

    def mkdags(self, project: Project) -> list[DAG]:
        """Generate mock DAGs for the project.
        :param project:
        :return:
        """
        # Create a list of DAGs
        dags = []
        for source in project.sources:
            dag = DAG(dag_id=f"{project.id}-dag-{source.id}", schedule=source.schedule)
            for _ in self.make_tasks(source):
                dag.add_task(_)

            dags.append(dag)
        return dags

    def make_tasks(self, source: DagConfig) -> list[BaseOperator]:
        """Generate the DAGs for the project."""
        ...


class NaiveMockDagGenerator(BaseDAGGenerator):
    """A Generates a DAG and ETL tasks per source. deprecated"""
    #  deprecated
    def make_tasks(self, source) -> list[BaseOperator]:
        """Create the tasks for the DAG."""
        for executor in source.executors:
            if executor.image == "airflow-operator://BashOperator":
                return [
                    BashOperator(retries=0, task_id=f"{source.id}-extract", bash_command=executor.commands, outlets=[Dataset(f"{source.id}-raw")]),
                    BashOperator(retries=0, task_id=f"{source.id}-transform", bash_command=executor.commands, inlets=[Dataset(f"{source.id}-raw")], outlets=[Dataset(f"{source.id}-processed")]),
                    BashOperator(retries=0, task_id=f"{source.id}-load", bash_command=executor.commands, inlets=[Dataset(f"{source.id}-processed")], outlets=[Dataset(f"{source.id}-loaded")]),
                ]
            elif executor.image == "airflow-operator://PythonOperator":
                return [
                    PythonOperator(retries=0, task_id=f"{source.id}-extract", python_callable=ensure_python_callable(executor.command), outlets=[Dataset(f"{source.id}-raw")]),
                    PythonOperator(retries=0, task_id=f"{source.id}-transform", python_callable=ensure_python_callable(executor.command), inlets=[Dataset(f"{source.id}-raw")], outlets=[Dataset(f"{source.id}-processed")]),
                    PythonOperator(retries=0, task_id=f"{source.id}-load", python_callable=ensure_python_callable(executor.command), inlets=[Dataset(f"{source.id}-processed")], outlets=[Dataset(f"{source.id}-loaded")]),
                ]
            else:
                # TODO - implement and test the TESOperator
                return [
                    TESOperator(retries=0, task_id=f"{source.id}-extract", tes_task=source, outlets=[Dataset(f"{source.id}-raw")]),
                    TESOperator(retries=0, task_id=f"{source.id}-transform", tes_task=source, inlets=[Dataset(f"{source.id}-raw")], outlets=[Dataset(f"{source.id}-processed")]),
                    TESOperator(retries=0, task_id=f"{source.id}-load", tes_task=source, inlets=[Dataset(f"{source.id}-processed")], outlets=[Dataset(f"{source.id}-loaded")]),
                ]


class AggregateLoadDagGenerator(BaseDAGGenerator):
    """Generates a set of DAGs with a DAG per source. see https://airflow.apache.org/docs/apache-airflow/2.5.0/concepts/dags.html#taskgroups"""

    def mkdags(self, project: Project) -> list[DAG]:
        """Generate mock DAGs for the project.
        :param project:
        :return:
        """
        # Create a list of DAGs
        assert project.sources, "Expected sources to be created"
        assert len(project.defaults.extract_commands) == 1
        assert len(project.defaults.transform_commands) == 1
        assert not project.defaults.load_commands, "Expected no load commands"
        dags = []
        processed_datasets = []
        loaded_dataset = Dataset(f"{project.id}-loaded")
        for source in project.sources:
            raw_dataset = Dataset(f"{source.id}-raw")
            processed_dataset = Dataset(f"{source.id}-processed")
            processed_datasets.append(processed_dataset)
            with DAG(
                    dag_id=f"{project.id}-{source.id}-extract",
                    default_args={'owner': 'airflow', },
                    schedule=None,
                    start_date=datetime(2024, 2, 1),
                    tags=["extract", project.id, source.id],
                    is_paused_upon_creation=True
            ) as extract_dag:

                parms = {
                    "retries": 0,
                    "task_id": f"{project.id}-{source.id}-extract",
                    "outlets": [raw_dataset]
                }

                extractors = self.ensure_operator(parms, executors=source.extract_commands)

                # ensure that each task in the extractors list is executed sequentially, with each task depending on the completion of the previous one.
                for i in range(len(extractors) - 1):
                    extractors[i] >> extractors[i + 1]

                dags.append(extract_dag)

            with DAG(
                    dag_id=f"{project.id}-{source.id}-transform",
                    default_args={'owner': 'airflow', },
                    schedule=[raw_dataset],
                    start_date=datetime(2024, 2, 1),
                    tags=["transform", project.id, source.id],
                    is_paused_upon_creation=True
            ) as transform_dag:

                parms = {
                    "retries": 0,
                    "task_id": f"{project.id}-{source.id}-transform",
                    "outlets": [processed_dataset],
                    "inlets": [raw_dataset]
                }
                transformers = self.ensure_operator(parms, executors=source.transform_commands)

                # ensure that each task in the transformers list is executed sequentially, with each task depending on the completion of the previous one.
                for i in range(len(transformers) - 1):
                    transformers[i] >> transformers[i + 1]

                dags.append(transform_dag)

        with DAG(
                dag_id=f"{project.id}-all-load",
                default_args={'owner': 'airflow', },
                schedule=processed_datasets,
                start_date=datetime(2024, 2, 1),
                tags=["load", project.id,] + [_.id for _ in project.sources],
                is_paused_upon_creation=True
        ) as load_dag:

            parms = {
                "retries": 0,
                "task_id": f"{project.id}-all-load",
                "outlets": [loaded_dataset],
                "inlets": processed_datasets
            }
            loaders = self.ensure_operator(parms, executors=project.load_commands, source=project)

            # ensure that each task in the loaders list is executed sequentially, with each task depending on the completion of the previous one.
            for i in range(len(loaders) - 1):
                loaders[i] >> loaders[i + 1]

            dags.append(load_dag)

        return dags

    def ensure_operator(self, operator_parms: dict, executors: list[tes.Executor] = None, source: tes.Task = None) -> list[BaseOperator]:
        """Ensure the operator is created, uses the TESOperator as default based on image is not known.

        Args:
            operator_parms: dictionary of parameters for the operator
            executors: list of executors to create the operators
            source: configuration tes.Task
        """
        operators = []
        if not executors:
            return operators

        original_operator_parms = deepcopy(operator_parms)
        original_task_id = original_operator_parms["task_id"]

        add_suffix = False
        suffix = 0

        if executors and len(executors) > 1:
            add_suffix = True

        for i, executor in enumerate(executors):
            _operator_parms = deepcopy(original_operator_parms)

            # Remove outlets and inlets if not the first or last executor
            if i != 0:
                _operator_parms.pop("inlets", None)
            if i != len(executors) - 1:
                _operator_parms.pop("outlets", None)

            assert isinstance(executor, tes.Executor), f"Expected tes.Executor, got {type(executor)}={executor}"
            if add_suffix:
                _operator_parms["task_id"] = f"{original_task_id}-{suffix}"
                suffix += 1

            log.info(f"Creating operator: {_operator_parms['task_id']} with image: {executor.image} {_operator_parms}")
            if executor.image == "airflow-operator://BashOperator":
                operator = BashOperator(**_operator_parms, bash_command=executor.command)
            elif executor.image == "airflow-operator://PythonOperator":
                operator = PythonOperator(**_operator_parms, python_callable=ensure_python_callable(executor.command))
            else:
                operator = TESOperator(**_operator_parms, tes_task=source)
            operators.append(operator)
        return operators

    def make_tasks(self, source) -> list[BaseOperator]:
        """Create the tasks for the DAG."""
        pass
