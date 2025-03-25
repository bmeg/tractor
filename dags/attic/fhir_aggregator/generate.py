from __future__ import annotations

import logging
import re

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from fhir_aggregator.config import load_default_config

log = logging.getLogger(__name__)

try:
    config = load_default_config()
except Exception as e:
    log.error(f"Error loading config : {e}")
    exit(0)


def projects():
    """Deprecated?"""
    hook = GCSHook()
    prefix = ""
    log.info(f"listing {config.bucket} {prefix}")
    manifest = hook.list(bucket_name=config.bucket, prefix=prefix)

    _projects = set([_.split("/")[0] for _ in manifest])
    # Remove items ending with 'md' or 'R4'
    _projects = {
        project for project in _projects if not re.search(r"(md|R4)$", project)
    }

    log.info(f"projects: {_projects}")


# projects()
