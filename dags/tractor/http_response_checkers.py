import hashlib
from typing import Any

from airflow.models import Variable
import requests
import logging
from airflow.models.taskinstance import SimpleTaskInstance


def check_md5(response: Any, dag_id: str = None, task_instance: SimpleTaskInstance = None) -> bool:
    """ Check the MD5 of a URL and store it in an Airflow Variable
    :param response: response object from the HTTP request
    :param dag_id: DAG ID to use for the Airflow Variable
    :param task_instance: TaskInstance that triggered the sensor
    """
    try:
        new_md5 = response.headers.get("Content-MD5")

        if not new_md5:
            logging.info(f"No MD5 found in response from {response.url} reading the content and calculating the MD5")
            new_md5 = hashlib.md5(response.content).hexdigest()
        if task_instance is not None:
            variable_name = f"{task_instance.dag_id}_md5"
        else:
            assert dag_id is not None, "Either dag_id or task_instance must be provided"
            variable_name = f"{dag_id}_md5"

        stored_md5 = Variable.get(variable_name, default_var=None)

        if stored_md5 != new_md5:
            logging.info(f"MD5 changed from {stored_md5} to {new_md5}")
            Variable.set(variable_name, new_md5, description=f"MD5 for {response.url}")
            return True  # Trigger the DAG

        logging.info(f"MD5 for {response.url} remains unchanged: {new_md5}")
        return False  # Keep waiting
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return False  # Retry on failure


def check_etag(response: Any, task_instance: SimpleTaskInstance, header_name: str = "ETag") -> bool:
    """ Check the ETag of a URL and store it in an Airflow Variable
    :param response: response object from the HTTP request
    :param task_instance: TaskInstance that triggered the sensor
    :param timeout: Timeout for the HTTP request
    :param header_name: Name of the header to check for the ETag default: "ETag"
    See https://airflow.apache.org/docs/apache-airflow-providers-http/stable/_api/airflow/providers/http/sensors/http/index.html#airflow.providers.http.sensors.http.HttpSensor
    """
    try:
        print(type(response))
        new_etag = response.headers.get(header_name)

        if not new_etag:
            logging.info(f"No ETag found in response from {response.url} -> Triggering the DAG")
            return True  # If no ETag, trigger the DAG

        variable_name = f"{task_instance.dag_id}_etag"
        stored_etag = Variable.get(variable_name, default_var=None)

        if stored_etag != new_etag:
            logging.info(f"ETag changed from {stored_etag} to {new_etag}")
            Variable.set(variable_name, new_etag, description=f"ETag for {response.url}")
            return True  # Trigger the DAG

        logging.info(f"ETag for {response.url} remains unchanged: {new_etag}")
        return False  # Keep waiting
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return False  # Retry on failure


def check_last_modified(url: str, variable_name: str, timeout: int = 60, header_name: str = "ETag") -> bool:
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
