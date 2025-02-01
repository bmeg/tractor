from datetime import datetime
import json
import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from hooks.test2_hook import Check, Extract, Transform, Load
from airflow.sensors.base import PokeReturnValue


@dag(
    schedule_interval="@daily",
    start_date=datetime.strptime("2025-01-01", "%Y-%m-%d"),
    catchup=False,
)
def test2_dag():
    test2_config = json.loads(Variable.get("test2_dag", default_var=None))

    @task.sensor(poke_interval=60, timeout=120, mode="poke", soft_fail=True)
    def check() -> PokeReturnValue:
        """Check if the ETag has changed"""
        condition_met = Check(test2_config).run()
        return PokeReturnValue(condition_met)

    @task
    def extract(poke_result: PokeReturnValue) -> bool:
        """Download from url, save in S3"""
        return Extract(test2_config).run()

    @task
    def transform(status: bool) -> bool:
        """Transform from json, save in S3 as html"""
        return Transform(test2_config).run()

    @task
    def load(status: bool) -> bool:
        """(mock) list the transformed files"""
        status = Load(test2_config).run()
        logging.info("Star Wars API catalog loaded to webserver")
        return status

    load(transform(extract(check())))  # set up the task dependencies


etl_workflow = test2_dag()
