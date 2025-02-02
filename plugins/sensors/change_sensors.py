from airflow.models import Variable
import requests
import logging


def check_etag(url: str, variable_name: str, timeout: int = 60, header_name: str = "ETag") -> bool:
    """ Check the ETag of a URL and store it in an Airflow Variable
    :param url: URL to check the ETag of
    :param variable_name: Name of the Airflow Variable to store the ETag
    :param timeout: Timeout for the HTTP request
    :param header_name: Name of the header to check for the ETag default: "ETag"
    """
    try:
        response = requests.head(url, timeout=timeout)
        response.raise_for_status()

        new_etag = response.headers.get(header_name)

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


def last_modified(url: str, variable_name: str, timeout: int = 60, header_name: str = "ETag") -> bool:
    """ Check the last-modified of a URL and store it in an Airflow Variable
    :param url: URL to check the last-modified of
    :param variable_name: Name of the Airflow Variable to store the last-modified
    :param timeout: Timeout for the HTTP request
    :param header_name: Name of the header to check for the last-modified default: "last-modified"
    """
    try:
        stored_value = Variable.get(variable_name, default_var=None)
        if stored_value:
            logging.info(f"Checking {url} with If-Modified-Since: {stored_value}")
            response = requests.head(url, timeout=timeout, headers={"If-Modified-Since": stored_value})
            response.raise_for_status()
            logging.info(f"status: {response.status_code} headers: {response.headers}")
            if response.status_code == 304:
                logging.info(f"last-modified remains unchanged: {stored_value}")
                return False

        response = requests.head(url, timeout=timeout)
        response.raise_for_status()
        new_value = response.headers.get(header_name)

        if not new_value:
            logging.info(f"No last-modified found in response from {url} -> Triggering the DAG")
            return True

        if stored_value != new_value:
            logging.info(f"last-modified changed from {stored_value} to {new_value}")
            Variable.set(variable_name, new_value, description=f"last-modified for {url}")
            return True

        logging.info(f"last-modified remains unchanged: {new_value}")
        return False  # Keep waiting
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return False  # Retry on failure
