import logging
import re
import urllib.parse
from copy import deepcopy
from typing import Optional

import tes
import yaml
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from pydantic import BaseModel, ConfigDict, model_validator

from tractor.etl_dsl.model import ETLProject, ETLProjectDefaults, ETLSource
from tractor.dag_helpers import blob_to_dict

from tractor.etl_dsl.generators.dag_generator import AggregateLoadDagGenerator

log = logging.getLogger(__name__)


def scan_bucket_for_sources(defaults) -> list[ETLSource]:  # pragma: no cover
    """Scan a bucket for likely sources."""
    hook = GCSHook()
    prefix = ""
    log.info(f"listing {defaults.bucket} {prefix}")

    manifest = hook.list(bucket_name=defaults.bucket, prefix=prefix)

    _sources = set([_.split("/")[0] for _ in manifest])

    # def set_defaults(_defaults, source):
    #     """Add defaults to a source."""
    #     _defaults = deepcopy(_defaults)
    #     for cmd in _defaults.transform_commands:
    #         executor: tes.Executor = cmd
    #         executor.command = executor.command.replace("{bucket_prefix}", source)
    #
    #     return _defaults

    # Remove items based on suffix
    _sources_list = [
        ETLSource(
            id=source,
            # name=source,
            # inputs=[StorageObject(url=f"gs://{defaults.bucket}/{source}")],
            # outputs=[StorageObject(url=f"gs://{defaults.bucket}/OUTPUT/R4/{source}")],
            # defaults=set_defaults(defaults, source)
        )
        for source in _sources
        if not re.search(r"(md|R4|OUTPUT|TEST|txt)$", source)
        and "TESTING" not in source
    ]

    return _sources_list


def get_source_manifest(outlets, outlet_events, *argc, **kwargs):  # pragma: no cover
    """Get source details."""
    assert len(outlets) == 1, (argc, kwargs)
    outlet = outlets[0]
    source_dict = outlet.extra["source"]
    inputs = source_dict["inputs"]
    expected_files = source_dict["defaults"].get("expected_files", [])

    assert len(inputs) == 1, inputs
    input_url = inputs[0]["url"]
    parsed_url = urllib.parse.urlparse(input_url)
    if parsed_url.scheme != "gs":
        raise ValueError("URL must start with 'gs://'")
    bucket = parsed_url.netloc
    path = parsed_url.path.lstrip("/")

    hook = GCSHook()
    prefix = path
    log.info(f"listing {bucket} {prefix}")
    manifest = hook.list(bucket_name=bucket, prefix=prefix)
    blobs = []
    for file in manifest:
        client = hook.get_conn()
        bucket_obj = client.bucket(bucket)
        blob = bucket_obj.get_blob(blob_name=file)
        extra_blob_info = {"bucket": bucket_obj.name, "gcp_conn_id": hook.gcp_conn_id}
        blobs.append(blob_to_dict(blob, extra=extra_blob_info))
    blob_names = [blob["name"] for blob in blobs]
    for expected_file in expected_files:
        expected_blob_name = f"{path}/{expected_file}"
        assert (
            expected_blob_name in blob_names
        ), f"Expected file {expected_blob_name} not found in {blob_names})"
    extra = {"manifest": blobs, "source": source_dict}
    outlet_events[outlet].extra = extra
    log.info(f"updated outlet: {outlet} with extra: {extra}")


class GRIPServer(BaseModel):
    """GRIP Server."""

    url: str


class GRIPDefaults(ETLProjectDefaults):
    """GRIP Default. We subclass ETLDefaults to add expected_files."""

    pass


class GRIPProject(ETLProject):
    """FHIR Aggregator Project. We subclass ETLProject to add FHIR Server."""

    grip_server: Optional[GRIPServer] = None
    defaults: GRIPDefaults

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def post_init(self):
        # Convert defaults dict to GRIPDefaults
        if self.defaults and isinstance(self.defaults, dict):
            self.defaults = GRIPDefaults(**self.defaults)

        # TODO - move this to etl_dsl.model.Project ?
        for source in self.sources:
            if isinstance(source, dict):
                # Merge source['defaults'] with self.defaults
                merged_defaults = {**self.defaults.model_dump(), **source["defaults"]}
                source["defaults"] = GRIPDefaults(**merged_defaults)

        assert self.grip_server is not None, "GRIP Server is required"

        return self


def create_project(config: dict) -> GRIPProject:
    """Create sources from config."""
    assert config, "should load config"
    assert "sources" in config, "should have sources"
    project = GRIPProject(**config)
    # check that the project was created
    assert project, "should create project"
    # and has at least one source
    assert len(project.sources) > 0, "should have sources"
    return project


def grip_dag(project, scrape_bucket=False) -> list[DAG]:
    """Generate DAGs for the project."""
    generator = AggregateLoadDagGenerator(project=project)
    return generator.mkdags()


def grip_config():
    _config = yaml.safe_load(
        """
id: grip

loader:
    id: "load"
    name: "Default Load Task"
    command: "echo Loading data"
    description: "Load data one"
    outputs:
    - minimal_project-loaded
    
defaults:
    bucket: bmeg
    bucket_prefix:
    bucket_scheme: s3
    operator_type: BashOperator
    extractor:
      id: "extract_1"
      name: "Default Extract Task"
      command: "echo Extracting data"
    transformer:
      id: "transform_1"
      name: "Default Transformer Task"
      command: "echo Transforming data"

sources:
- id: gdc
- id: gdsc
- id: ccle
- id: go
- id: pharmacodb
- id: dgidb
- id: meta
- id: ensembl
- id: gene_enricher
- id: mondo
- id: msigdb
  
grip_server:
    url: https://TODO

    """
    )
    return _config
