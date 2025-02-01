import json
import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from hooks.swapi_hook import CheckETag, Extract, Transform, Load
from airflow.sensors.base import PokeReturnValue


@dag(schedule_interval="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def swapi_dag():
    swapi_config = json.loads(Variable.get("swapi_dag", default_var=None))

    @task.sensor(poke_interval=60, timeout=120, mode="poke", soft_fail=True)
    def check_etag() -> PokeReturnValue:
        """ Check if the ETag of the Star Wars API has changed.
         If the ETag has changed, the DAG will be triggered.
         Wait 60 seconds between pokes, and timeout after 120 seconds.
        """
        # condition_met = ETagChangeSensor(url=swapi_config["url"], task_id="check_etag",
        #                                  etag_var_name="swapi_etag").poke(None)
        condition_met = CheckETag(swapi_config).run()
        return PokeReturnValue(condition_met)

    @task
    def extract(poke_result: PokeReturnValue) -> bool:
        """ Extract the Star Wars API catalog to S3 """
        logging.info(f"ETag changed: {poke_result}")
        return Extract(swapi_config).run()

    @task
    def transform(status: bool) -> bool:
        """ Transform the Star Wars API to HTML, stored in S3 """
        return Transform(swapi_config).run()

    @task
    def load(status: bool) -> bool:
        """ (mock) Load the Star Wars API catalog to the webserver """
        status = Load(swapi_config).run()
        logging.info("Star Wars API catalog loaded to webserver")
        return status

    load(transform(extract(check_etag())))  # set up the task dependencies


etl_workflow = swapi_dag()
