import logging
import re
import urllib.parse
from copy import deepcopy

import tes
import yaml
import attr
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from tractor.config import Project, DagConfig, Default
from tractor.dag_generator import AggregateLoadDagGenerator
from tractor.dag_helpers import blob_to_dict

log = logging.getLogger(__name__)


def scan_bucket_for_sources(defaults) -> list[DagConfig]:
    """Scan a bucket for likely sources."""
    hook = GCSHook()
    prefix = ""
    log.info(f"listing {defaults.bucket} {prefix}")

    manifest = hook.list(bucket_name=defaults.bucket, prefix=prefix)

    _sources = set([_.split("/")[0] for _ in manifest])

    def set_defaults(_defaults, source):
        """Add defaults to a source."""
        _defaults = deepcopy(_defaults)
        for cmd in _defaults.transform_commands:
            executor: tes.Executor = cmd
            executor.command = executor.command.replace("{bucket_prefix}", source)

        return _defaults

    # Remove items based on suffix

    _sources = [
        DagConfig(**{'id': source, 'name': source, 'inputs': [source], 'outputs': [f"OUTPUT/R4/{source}"],  'defaults': set_defaults(defaults, source)}) for source in _sources if not re.search(r"(md|R4|OUTPUT|TEST|txt)$", source) and 'TESTING' not in source
    ]

    return _sources


def get_source_manifest(outlets, outlet_events, *argc, **kwargs):
    """Get source details."""
    assert len(outlets) == 1, (argc, kwargs)
    outlet = outlets[0]
    source_dict = outlet.extra['source']
    inputs = source_dict['inputs']
    expected_files = source_dict['defaults'].get('expected_files', [])

    assert len(inputs) == 1, inputs
    input_url = inputs[0]['url']
    parsed_url = urllib.parse.urlparse(input_url)
    if parsed_url.scheme != 'gs':
        raise ValueError("URL must start with 'gs://'")
    bucket = parsed_url.netloc
    path = parsed_url.path.lstrip('/')

    hook = GCSHook()
    prefix = path
    log.info(f"listing {bucket} {prefix}")
    manifest = hook.list(bucket_name=bucket, prefix=prefix)
    blobs = []
    for file in manifest:
        client = hook.get_conn()
        bucket_obj = client.bucket(bucket)
        blob = bucket_obj.get_blob(blob_name=file)
        extra_blob_info = {
            "bucket": bucket_obj.name,
            "gcp_conn_id": hook.gcp_conn_id}
        blobs.append(blob_to_dict(blob, extra=extra_blob_info))
    blob_names = [blob['name'] for blob in blobs]
    for expected_file in expected_files:
        expected_blob_name = f"{path}/{expected_file}"
        assert expected_blob_name in blob_names, f"Expected file {expected_blob_name} not found in {blob_names})"
    extra = {
        "manifest": blobs,
        "source": source_dict
    }
    outlet_events[outlet].extra = extra
    log.info(f"updated outlet: {outlet} with extra: {extra}")


@attr.define
class FHIRServer:
    url: str


@attr.define
class FHIRAggregatorDefaults(Default):
    """FHIR Aggregator Default. We subclass Default to add expected_files."""
    expected_files: list[str] = []

    def __attrs_post_init__(self):
        super().__attrs_post_init__()


@attr.define
class FHIRAggregatorProject(Project):
    """FHIR Aggregator Project. We subclass Project to add FHIR Server."""
    fhir_server: FHIRServer = None
    defaults: FHIRAggregatorDefaults = attr.ib(factory=FHIRAggregatorDefaults)

    def __attrs_post_init__(self):
        # Convert defaults dict to FHIRAggregatorDefaults
        if self.defaults and isinstance(self.defaults, dict):
            self.defaults = FHIRAggregatorDefaults(**self.defaults)

        # TODO - move this to tractor.config.Project ?
        for source in self.sources:
            if isinstance(source, dict):
                # Merge source['defaults'] with self.defaults
                merged_defaults = {**attr.asdict(self.defaults), **source['defaults']}
                source['defaults'] = FHIRAggregatorDefaults(**merged_defaults)
                for cmd in source['defaults'].transform_commands:
                    executor: tes.Executor = cmd
                    executor.command = executor.command.replace("{bucket_prefix}", source['id'])

        super().__attrs_post_init__()
        assert self.fhir_server is not None, "FHIR Server is required"

        # Add sources
        configured_source_ids = [_.id for _ in self.sources]
        self.sources.extend([_ for _ in scan_bucket_for_sources(self.defaults) if _.id not in configured_source_ids])


def test_config():
    _config = yaml.safe_load("""
id: fhir-aggregator

load_commands:
    - echo "load-all"

defaults:
    bucket: fhir-aggregator-public
    bucket_prefix:
    bucket_scheme: gs
    operator_type: airflow-operator://BashOperator
    extract_commands:
      - command: fhir_aggregator_generate.get_source_manifest
        image: airflow-operator://PythonOperator
    transform_commands:
        - "pwd; fa_submit prep {bucket_prefix}/META OUTPUT/R4/{bucket_prefix}/META --transformers part-of,vocabulary,validate --fhir-version R4"
    expected_files:
    - META/ResearchStudy.ndjson
    - META/ResearchSubject.ndjson
    - META/Patient.ndjson
    - META/DocumentReference.ndjson

sources:
  # other sources added dynamically by scanning bucket 
  - id: IG
    # The IG directory is a special case, it currently only has search parameters
    name: Implementation Guide
    inputs:
      - IG
    outputs:
      - OUTPUT/R4/IG
    defaults:
        transform_commands:
            - "pwd; ls -l IG/META ; mkdir -p OUTPUT/R4/IG;  cp -r IG/META OUTPUT/R4/IG/META ; ls -l OUTPUT/R4/IG/META"
        expected_files:
          - META/part-of-search-parameter.ndjson


fhir_server:
    url: https://TODO
    
    """
    )
    return _config


def create_project(config: dict) -> FHIRAggregatorProject:
    """Create sources from config."""
    assert config, "should load config"
    assert 'sources' in config, "should have sources"
    project = FHIRAggregatorProject(**config)
    # check that the project was created
    assert project, "should create project"
    # and has at least one source
    assert len(project.sources) > 0, "should have sources"
    for _ in project.sources:
        if _.id == 'IG':
            assert _.defaults.expected_files == ['META/part-of-search-parameter.ndjson'], _.defaults.expected_files
    return project


def fhir_aggregator_dag(project) -> list[DAG]:
    return AggregateLoadDagGenerator().mkdags(project)


config = test_config()
dags = fhir_aggregator_dag(create_project(config))
for _ in dags:
    _
