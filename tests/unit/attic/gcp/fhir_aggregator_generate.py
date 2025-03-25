import logging
import re

import yaml
import attr
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from tractor.config import Project, DagConfig

log = logging.getLogger(__name__)


def sources(config) -> list[DagConfig]:
    """Scan a bucket for likely sources."""
    hook = GCSHook()
    prefix = ""
    log.info(f"listing {config.bucket} {prefix}")
    manifest = hook.list(bucket_name=config.bucket, prefix=prefix)

    _sources = set([_.split("/")[0] for _ in manifest])
    # Remove items ending with 'md' or 'R4'
    _sources = [
        DagConfig({'id': source, 'inputs': [source]}) for source in _sources if not re.search(r"(md|R4|OUTPUT|TEST)$", source)
    ]

    log.info(f"_sources: {_sources}")
    return _sources


@attr.define
class FHIRServer:
    url: str


@attr.define
class FHIRAggregatorProject(Project):
    fhir_server: FHIRServer = None

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        assert self.fhir_server is not None, "FHIR Server is required"
        self.sources = sources(self.defaults)


def config():
    return yaml.safe_load("""
id: fhir-aggregator

load_commands:
    - echo "load-all"

defaults:
    bucket: fhir-aggregator-public
    bucket_prefix:
    bucket_scheme: gs
    operator_type: BashOperator
    extract_commands:
        - echo "extract"
    transform_commands:
        - "pwd;fa_submit prep {bucket_prefix}/META OUTPUT/R4/{bucket_prefix}/META --transformers part-of,vocabulary,validate --fhir-version R4"

sources: []


fhir_server:
    url: https://TODO
    
    """
    )


def create_sources(config: dict):
    """Create sources from config."""
    assert config, "should load config"
    assert 'sources' in config, "should have sources"
    project = FHIRAggregatorProject(**config)
    assert project, "should create project"
    log.info(f"Project: {project}")


create_sources(config())
