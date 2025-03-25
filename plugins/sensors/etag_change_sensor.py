from airflow.models import Variable
import requests
import logging


def check_etag(url: str, variable_name: str, timeout: int = 60) -> bool:
    """ Check the ETag of a URL and store it in an Airflow Variable
    :param url: URL to check the ETag of
    :param variable_name: Name of the Airflow Variable to store the ETag
    :param timeout: Timeout for the HTTP request
    """
    try:
        response = requests.head(url, timeout=timeout)
        response.raise_for_status()

        new_etag = response.headers.get("ETag")

        if not new_etag:
            logging.info(f"No ETag found in response from {url} -> Triggering the DAG")
            return True  # If no ETag, trigger the DAG

        stored_etag = Variable.get(variable_name, default_var=None)

        if stored_etag != new_etag:
            logging.info(f"ETag changed from {stored_etag} to {new_etag}")
            Variable.set(variable_name, new_etag, description=f"ETag for {url}")
            return True  # Trigger the DAG

        logging.info(f"ETag remains unchanged: {new_etag}")
        return False  # Keep waiting
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return False  # Retry on failure
