import logging
from typing import Any

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from dateutil.parser import parse

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


# Function to get the first character after the "stapi-" prefix
def get_first_char(name):
    return name.replace("stapi-", "")[0]


# Divide into 4 bins
bins = {
    "A-C": [name for name in dataset_names if "A" <= get_first_char(name) <= "C"],
    "D-L": [name for name in dataset_names if "D" <= get_first_char(name) <= "L"],
    "M-S": [name for name in dataset_names if "M" <= get_first_char(name) <= "S"],
    "T-Z": [name for name in dataset_names if "T" <= get_first_char(name) <= "Z"],
}

dataset_input_bins = {}
for k, v in bins.items():
    dataset_input_bins[f"stapi-bin-{k}"] = [Dataset(_) for _ in v]
dataset_combined_bins = {
    f"{key}-combined": Dataset(f"{key}-combined") for key in dataset_input_bins.keys()
}

# Define a dag that for each bin of datasets


def bin_datasets(inlet_events, outlet_events, input_bins, output_bin):
    binned_manifest = []
    for dataset in input_bins:
        logging.info(f"Processing dataset: {dataset}")
        events = inlet_events[dataset]
        if events is None or len(events) == 0:
            logging.warning(f"No events found for {dataset}")
            continue
        last_event = events[-1]
        assert last_event, f"No events found for {dataset}"
        assert "manifest" in last_event.extra, f"No manifest found in {last_event}"
        manifest = last_event.extra["manifest"]
        logging.info(f"Extracted data: {manifest}")
        binned_manifest.extend(manifest)
    outlet_events[output_bin].extra = {"manifest": binned_manifest}
    return binned_manifest


@dag(
    catchup=False,
    dag_id=f"stapi-bin-A-C-LOADER",
    description=f"Loader for Star Trek API A-C",
    start_date=parse("2025-01-01"),
    schedule=dataset_input_bins[f"stapi-bin-A-C"],
    tags=["LOADER"],
)
def bin_dag_a_c():
    """Star Trek binner"""
    input_bins = dataset_input_bins[f"stapi-bin-A-C"]
    output_bin = dataset_combined_bins[f"stapi-bin-A-C-combined"]

    @task(inlets=input_bins, outlets=[output_bin])
    def binner(inlet_events, outlet_events, *args, **kwargs) -> Any:
        """Load the data"""
        binned_manifest = bin_datasets(
            inlet_events, outlet_events, input_bins, output_bin
        )
        return {"loaded": len(binned_manifest)}

    # set up the task dependencies
    binner()


bin_dag_a_c()


@dag(
    catchup=False,
    dag_id=f"stapi-bin-D-L-LOADER",
    description=f"Loader for Star Trek API D-L",
    start_date=parse("2025-01-01"),
    schedule=dataset_input_bins[f"stapi-bin-D-L"],
    tags=["LOADER"],
)
def bin_dag_d_l():
    """Star Trek binner"""
    input_bins = dataset_input_bins[f"stapi-bin-D-L"]
    output_bin = dataset_combined_bins[f"stapi-bin-D-L-combined"]

    @task(inlets=input_bins, outlets=[output_bin])
    def binner(inlet_events, outlet_events, *args, **kwargs) -> Any:
        """Load the data"""
        # get the manifest from the event history for the processed_dataset
        binned_manifest = bin_datasets(
            inlet_events, outlet_events, input_bins, output_bin
        )
        return {"loaded": len(binned_manifest)}

    # set up the task dependencies
    binner()


bin_dag_d_l()


@dag(
    catchup=False,
    dag_id=f"stapi-bin-M-S-LOADER",
    description=f"Loader for Star Trek API M-S",
    start_date=parse("2025-01-01"),
    schedule=dataset_input_bins[f"stapi-bin-M-S"],
    tags=["LOADER"],
)
def bin_dag_m_s():
    """Star Trek binner"""
    input_bins = dataset_input_bins[f"stapi-bin-M-S"]
    output_bin = dataset_combined_bins[f"stapi-bin-M-S-combined"]

    @task(inlets=input_bins, outlets=[output_bin])
    def binner(inlet_events, outlet_events, *args, **kwargs) -> Any:
        """Load the data"""
        # get the manifest from the event history for the processed_dataset
        binned_manifest = bin_datasets(
            inlet_events, outlet_events, input_bins, output_bin
        )
        return {"loaded": len(binned_manifest)}

    # set up the task dependencies
    binner()


bin_dag_m_s()


@dag(
    catchup=False,
    dag_id=f"stapi-bin-T-Z-LOADER",
    description=f"Loader for Star Trek API T-Z",
    start_date=parse("2025-01-01"),
    schedule=dataset_input_bins[f"stapi-bin-T-Z"],
    tags=["LOADER"],
)
def bin_dag_t_z():
    """Star Trek binner"""
    input_bins = dataset_input_bins[f"stapi-bin-T-Z"]
    output_bin = dataset_combined_bins[f"stapi-bin-T-Z-combined"]

    @task(inlets=input_bins, outlets=[output_bin])
    def binner(inlet_events, outlet_events, *args, **kwargs) -> Any:
        """Load the data"""
        # get the manifest from the event history for the processed_dataset
        binned_manifest = bin_datasets(
            inlet_events, outlet_events, input_bins, output_bin
        )
        return {"loaded": len(binned_manifest)}

    # set up the task dependencies
    binner()


bin_dag_t_z()


@dag(
    catchup=False,
    dag_id="stapi-LOADER",
    description="Loader for Star Wars API",
    start_date=parse("2025-01-01"),
    schedule=dataset_combined_bins.values(),
    tags=["LOADER"],
)
def loader():
    """Star Wars Loader"""

    # Define the tasks

    @task(inlets=dataset_combined_bins.values())
    def load(inlet_events, *args, **kwargs) -> Any:
        """Load the data"""
        # get the manifest from the event history for the raw_dataset
        combined_manifest = []
        for dataset in dataset_combined_bins.values():
            events = inlet_events[dataset]
            if events is None or len(events) == 0:
                logging.warning(f"No events found for {dataset}")
                continue
            last_event = events[-1]
            assert last_event, f"No events found for {dataset}"
            assert "manifest" in last_event.extra, f"No manifest found in {last_event}"
            manifest = last_event.extra["manifest"]
            logging.info(f"Extracted data: {manifest}")
            combined_manifest.extend(manifest)
        return {"loaded": len(combined_manifest)}

    # set up the task dependencies
    load()


# instantiate the DAG
loader()
logging.info("stapi-Loader DAG instantiated")
