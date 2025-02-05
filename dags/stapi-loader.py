
import logging
import json
from typing import Any
from dateutil.parser import parse
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.datasets import Dataset

dataset_names = """
stapi-Animal_processed
stapi-AstronomicalObject_processed
stapi-Book_processed
stapi-BookCollection_processed
stapi-BookSeries_processed
stapi-Character_processed
stapi-ComicCollection_processed
stapi-ComicSeries_processed
stapi-ComicStrip_processed
stapi-Comics_processed
stapi-Company_processed
stapi-Conflict_processed
stapi-Element_processed
stapi-Episode_processed
stapi-Food_processed
stapi-Literature_processed
stapi-Location_processed
stapi-Magazine_processed
stapi-MagazineSeries_processed
stapi-Material_processed
stapi-MedicalCondition_processed
stapi-Movie_processed
stapi-Occupation_processed
stapi-Organization_processed
stapi-Performer_processed
stapi-Season_processed
stapi-Series_processed
stapi-Soundtrack_processed
stapi-Spacecraft_processed
stapi-SpacecraftClass_processed
stapi-Species_processed
stapi-Staff_processed
stapi-Swagger_processed
stapi-Technology_processed
stapi-Title_processed
stapi-TradingCard_processed
stapi-TradingCardDeck_processed
stapi-TradingCardSet_processed
stapi-VideoGame_processed
stapi-VideoRelease_processed
stapi-Weapon_processed
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
            if events is None or len(events) == 0:
                logging.warning(f"No events found for {dataset}")
                continue
            last_event = events[-1]
            assert last_event, f"No events found for {config.raw_dataset}"
            assert 'manifest' in last_event.extra, f"No manifest found in {last_event}"
            manifest = last_event.extra['manifest']
            logging.info(f"Extracted data: {manifest}")
        return {"loaded": len(datasets)}

    # set up the task dependencies
    load()


# instantiate the DAG
loader()
logging.info("stapi-Loader DAG instantiated")
