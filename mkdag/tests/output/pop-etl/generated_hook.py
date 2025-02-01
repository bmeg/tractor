from typing import Any

from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from pydantic import BaseModel

# application code


class Test2Config(BaseModel):
    """A POP (plain old python) ETL pipeline. Use Pydantic for config validation"""

    aws_conn_id: str
    bucket_name: str
    etag_variable_name: str
    s3_prefix: str
    url: str


class Test2Hook(BaseHook):
    """A POP (plain old python) ETL pipeline"""

    def __init__(self, config_dict: dict[str, Any]):
        # require a config
        if not config_dict:
            raise ValueError("config is required")
        # parse the config
        self.config = Test2Config(**config_dict)
        # create the S3 hook
        # for now, assume everything needs a connection
        self.s3_hook = S3Hook(aws_conn_id=self.config.aws_conn_id)

    def run(self) -> bool:
        """Run the ETL task"""
        raise NotImplementedError("Subclasses must implement the run method")


class Check(Test2Hook):
    """Generated sensor"""

    def run(self) -> bool:
        """Check if the ETag has changed"""
        pass


class Extract(Test2Hook):
    """Generated extractor"""

    def run(self) -> bool:
        """Download from url, save in S3"""
        pass


class Transform(Test2Hook):
    """Generated transformer class"""

    def run(self) -> bool:
        """Transform from json, save in S3 as html"""
        pass


class Load(Test2Hook):
    """Generated load class"""

    def run(self) -> bool:
        """(mock) list the transformed files"""
        pass
