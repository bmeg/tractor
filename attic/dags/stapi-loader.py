
import logging
import json
from typing import Any
from dateutil.parser import parse
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.datasets import Dataset

dataset_names = """
stapi-Animal-processed
stapi-AstronomicalObject-processed
stapi-Book-processed
stapi-BookCollection-processed
stapi-BookSeries-processed
stapi-Character-processed
stapi-ComicCollection-processed
stapi-ComicSeries-processed
stapi-ComicStrip-processed
stapi-Comics-processed
stapi-Company-processed
stapi-Conflict-processed
stapi-Element-processed
stapi-Episode-processed
stapi-Food-processed
stapi-Literature-processed
stapi-Location-processed
stapi-Magazine-processed
stapi-MagazineSeries-processed
stapi-Material-processed
stapi-MedicalCondition-processed
stapi-Movie-processed
stapi-Occupation-processed
stapi-Organization-processed
stapi-Performer-processed
stapi-Season-processed
stapi-Series-processed
stapi-Soundtrack-processed
stapi-Spacecraft-processed
stapi-SpacecraftClass-processed
stapi-Species-processed
stapi-Staff-processed
stapi-Swagger-processed
stapi-Technology-processed
stapi-Title-processed
stapi-TradingCard-processed
stapi-TradingCardDeck-processed
stapi-TradingCardSet-processed
stapi-VideoGame-processed
stapi-VideoRelease-processed
stapi-Weapon-processed
""".split()

datasets = [Dataset(_) for _ in dataset_names]


@dag(
    catchup=False,
    dag_id="stapi-LOADER",
    description="Loader for Star Wars API",
    start_date=parse("2025-01-01"),
    schedule=datasets
)
def loader():
    """Star Wars Loader"""

    # Define the tasks

    @task(inlets=datasets)
    def load(inlet_events, *args, **kwargs) -> Any:
        """Load the data"""
        # get the manifest from the event history for the raw_dataset
        for dataset in datasets:
            events = inlet_events[dataset]
            last_event = events[-1]
            assert last_event, f"No events found for {config.raw_dataset}"
            assert 'manifest' in last_event.extra, f"No manifest found in {last_event}"
            manifest = last_event.extra['manifest']
            logging.info(f"Extracted data: {manifest}")
        return {"loaded": len(datasets)}

    # set up the task dependencies
    load


# instantiate the DAG
loader()
logging.info("stapi-Loader DAG instantiated")
