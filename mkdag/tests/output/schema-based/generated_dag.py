from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="simple_gene_processing_dag",
    schedule="@daily",
    start_date=datetime.strptime("2025-01-01", "%Y-%m-%d"),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": 5 * 60,
    },
)
def simple_gene_processing_dag():

    @task
    def extract():

        from tasks import extract_data

        return extract_data()

    @task
    def transform(extract):

        from tasks import transform_data

        return transform_data(extract)

    @task
    def load(transform):

        import subprocess

        subprocess.run("echo &#39;Loading data completed&#39;", shell=True, check=True)

    transform.set_upstream(extract)

    load.set_upstream(transform)


simple_gene_processing_dag()
