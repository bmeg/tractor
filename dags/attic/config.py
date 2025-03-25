import logging
import pathlib
from typing import Any, Protocol

import attr
import tes
from airflow import Dataset, DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from tractor.tes_operator import TESOperator

logger = logging.getLogger(__name__)


@attr.define
class Default:
    """Defaults for  a project."""

    bucket_connection_id: str | None = None
    """The default connection id, used by an actor to connect with the bucket."""

    bucket: str | None = None
    """The default bucket name for input and output."""

    bucket_prefix: str | None = None
    """The default bucket name for input and output."""

    bucket_scheme: str | None = None
    """The default bucket scheme."""

    operator_type: str = "airflow-operator://BashOperator"
    """The default operator type. Coordinate with DAG generator to ensure this is used."""

    extract_commands: list[str] | list[dict] | list[tes.Executor] | None = None
    """The commands to run to download datasets complete."""

    transform_commands: list[str] | list[dict] | list[tes.Executor] | None = None
    """The commands to run when all extract commands complete."""

    load_commands: list[str] | list[dict] | list[tes.Executor] | None = None
    """The commands to run when all transform commands complete."""

    def __attrs_post_init__(self):
        """Apply defaults after initialization."""
        ensure_etl_commands(self, self.operator_type)


@attr.define
class DagConfig(tes.Task):
    """An adapter between the tractor Config File the tes Task.

    This class is responsible for loading the configuration file and applying defaults
    where necessary. It makes it simpler for the user to specify minimal configuration.
    This class expands that minimal configuration to a full configuration.

    See https://github.com/ga4gh/task-execution-schemas/blob/develop/openapi/task_execution_service.openapi.yaml#L762
    """

    # a new field, not used in TES, we will adapt it to Airflow expected values
    schedule: str | list[str] | list[Dataset] | None = None
    """See https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#airflow.models.dag.ScheduleArg"""

    # an existing field, used in TES, we will adapt it to TES expected values
    inputs: list[Any] = attr.ib(factory=list)
    """See ..."""

    # an existing field, used in TES, we will adapt it to TES expected values
    outputs: list[Any] = attr.ib(factory=list)
    """See ..."""

    # a new field, not used in TES, we will use it to expand user provided values
    defaults: Default | dict = attr.ib(factory=Default)
    """The default configuration for the project."""

    # new fields, not used in TES, we will use it to populate tes Executors
    extract_commands: list[str] | list[dict] | list[tes.Executor] | None = None
    """The commands to run to download datasets complete."""

    transform_commands: list[str] | list[dict] | list[tes.Executor] | None = None
    """The commands to run when all extract commands complete."""

    load_commands: list[str] | list[dict] | list[tes.Executor] | None = None
    """The commands to run when all transform commands complete."""

    def __attrs_post_init__(self):
        """Apply defaults after initialization."""
        self.apply_defaults()

    def apply_defaults(self):
        """Apply default values to the configuration."""
        self.ensure_id_and_name()
        self.ensure_schedule()
        self.ensure_inputs()
        self.ensure_outputs()
        if not self.load_commands:
            self.load_commands = self.defaults.load_commands
        if not self.extract_commands:
            self.extract_commands = self.defaults.extract_commands
        if not self.transform_commands:
            self.transform_commands = self.defaults.transform_commands
        ensure_etl_commands(self, self.defaults.operator_type)

    def ensure_schedule(self):
        """Ensure that the schedule is set."""
        # already set
        if not self.schedule:
            return
        # already set
        if all([type(_) is Dataset for _ in self.schedule]):
            return
        # simple string
        if isinstance(self.schedule, str):
            return
        if all([type(_) is str for _ in self.schedule]):
            self.schedule = [Dataset(_) for _ in self.schedule]
            return
        # unexpected type
        raise TypeError(f"Unexpected type for schedule: {type(self.schedule)} {self.schedule}")

    def ensure_inputs(self):
        """Adapt simplified config."""
        # TODO - inputs and outputs should have same ensure method
        if not self.inputs:
            return
            # raise TypeError("Expected inputs")
        task_inputs = []
        for _input in self.inputs:
            # already a TES Input
            if isinstance(_input, tes.Input):
                task_inputs.append(_input)
                continue
            # already a dict
            if isinstance(_input, dict):
                task_inputs.append(tes.Input(**_input))
                continue
            # not a string
            if not isinstance(_input, str):
                raise TypeError(f"Expected a string, got {_input}")
            # simple parsing
            parsed = pathlib.Path(_input)
            _type = "FILE"
            # no suffix, must be a directory
            if not parsed.suffix:
                _type = "DIRECTORY"

            url = f"{self.default_bucket_scheme()}://{self.default_bucket_name()}/{_input}"

            task_inputs.append(
                tes.Input(
                    name=parsed.name,
                    type=_type,
                    url=url,
                    path=_input,
                    description="Generated from config file",
                )
            )
        self.inputs = task_inputs

    def ensure_outputs(self):
        """Adapt simplified config."""
        # TODO - inputs and outputs should have same ensure method
        if self.outputs:
            task_outputs = []
            for output in self.outputs:
                # already a TES Output
                if isinstance(output, tes.Output):
                    task_outputs.append(output)
                    continue
                # already a dict
                if isinstance(output, dict):
                    task_outputs.append(tes.Output(**output))
                    continue
                # not a string
                if not isinstance(output, str):
                    raise TypeError(f"Expected a string, got {output}")
                # simple parsing
                parsed = pathlib.Path(output)
                _type = "FILE"
                # no suffix, must be a directory
                if not parsed.suffix:
                    _type = "DIRECTORY"

                url = f"{self.default_bucket_scheme()}://{self.default_bucket_name()}/{output}"

                task_outputs.append(
                    tes.Output(
                        name=parsed.name,
                        type=_type,
                        url=url,
                        path=output,
                        description="Generated from config file",
                    )
                )
            self.outputs = task_outputs

    def default_bucket_scheme(self) -> str:
        """Return the default bucket scheme."""
        # TODO - make more realistic
        if not self.defaults.bucket_scheme:
            raise TypeError(f"Expected a default bucket scheme {self.defaults}")
        return self.defaults.bucket_scheme

    def default_bucket_name(self):
        """Return the default bucket name."""
        # TODO - make more realistic
        if not self.defaults.bucket:
            raise TypeError("Expected a default bucket name")
        return self.defaults.bucket

    def ensure_id_and_name(self):
        # TODO - make more realistic
        if not self.id or not self.name:
            raise TypeError("Both id and name must be provided and non-empty")

    def ensure_commands(self):
        """Populate the tes Executors."""
        self.extract_commands = ensure_commands(self.extract_commands, self.defaults.operator_type)
        self.transform_commands = ensure_commands(self.transform_commands, self.defaults.operator_type)
        self.load_commands = ensure_commands(self.load_commands, self.defaults.operator_type)


@attr.define
class Project(tes.Task):
    """A project configuration."""

    defaults: Default = attr.ib(factory=Default)
    """The default configuration for the project."""

    sources: list[DagConfig] = attr.ib(factory=list)
    """The configured data sources for the project."""

    load_commands: list[str] | list[dict] | list[tes.Executor] | None = None
    """The commands to run when all source DAGS complete."""

    def __attrs_post_init__(self):
        """Apply defaults after initialization."""
        self.ensure_defaults()
        self.ensure_sources()
        self.load_commands = ensure_commands(self.load_commands, self.defaults.operator_type)

    def ensure_defaults(self):
        """Ensure that the defaults for projects are set."""
        if self.defaults and isinstance(self.defaults, dict):
            self.defaults = Default(**self.defaults)
        elif not self.defaults:
            self.defaults = Default()

    def ensure_sources(self):
        """Ensure that the sources are set."""
        sources = []
        for source in self.sources:
            if isinstance(source, DagConfig):
                sources.append(source)
                continue
            if not isinstance(source, dict):
                raise TypeError(f"Expected a DagConfig or dict, got {source}")
            if 'id' not in source:
                source['id'] = self.id
            if 'name' not in source:
                source['name'] = f"{self.name or self.id}"
            if 'defaults' not in source:
                source['defaults'] = self.defaults
            sources.append(DagConfig(**source))
        self.sources = sources


def ensure_etl_commands(source_or_defaults: DagConfig | Default, default_operator: str):
    """Populate the tes Executors."""
    source_or_defaults.extract_commands = ensure_commands(source_or_defaults.extract_commands, default_operator)
    source_or_defaults.transform_commands = ensure_commands(source_or_defaults.transform_commands, default_operator)
    source_or_defaults.load_commands = ensure_commands(source_or_defaults.load_commands, default_operator)


def ensure_commands(commands: list[str] | list[dict] | list[tes.Executor] | None, default_operator: str = "airflow-operator://BashOperator") -> list[tes.Executor] | None:
    """Set executors that will run after all sources."""
    # TODO - make more elegant and realistic
    if not commands:
        return []
    executors = []
    for command in commands:
        # already set
        if not isinstance(command, tes.Executor):
            # simple string
            if isinstance(command, str):
                command = default_command({"command": command}, default_operator)
            # simple dict
            elif isinstance(command, dict):
                command = default_command(command, default_operator)
            # unexpected type
            else:
                raise TypeError(f"Unexpected type for command: {type(command)} {command}")
        executors.append(command)

    return executors


def default_command(data: dict | str, default_operator: str) -> tes.Executor:
    """Create a Default object from a dictionary."""
    if not data:
        # TODO - coordinate and document expectation that the DAG generator realize the `image` field is over-ridden to indicate the operator
        data = {"command": "echo hello", "image": default_operator}
    elif isinstance(data, str):
        data = {"command": data, "image": default_operator}
    if not data.get("image"):
        data["image"] = default_operator
    return tes.Executor(**data)
