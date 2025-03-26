import logging
import os
import shutil
from collections import defaultdict

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.state import State

log = logging.getLogger(__name__)


def list_files_recursive(directory) -> list[str]:
    """
    List all files in a directory and its subdirectories.

    Args:
        directory (str): The root directory to list files from.

    Returns:
        list[str]: A list of file paths relative to the root directory.
    """
    file_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_paths.append(
                str(os.path.join(root, file)).replace(directory + "/", "", 1)
            )
    return file_paths


def on_failure_callback(context):
    """
    Callback function to handle task failure, cleanup tmp_dir.

    Args:
        context (dict): The context dictionary provided by Airflow.
    """
    cwd = os.getcwd()
    state = context["ti"].state
    log.warning(
        f"on_failure_callback:\nstate: {state}\ncwd: {cwd}\ncontext: {context}\nti: {context['ti']}"
    )
    # Clean up the temporary directory
    shutil.rmtree(context["tmp_dir"])
    log.info(f"Removed {context['tmp_dir']}")


def on_success_callback(context):
    """
    Upload the output to GCS, Update the outlets with the manifest, Clean up the temporary directory.

    Args:
        context (dict): The context dictionary provided by Airflow.
    """
    try:
        cwd = os.getcwd()
        log.info(
            f"on_success_callback\ncwd: {cwd}\ncontext: {context}\nti: {context['ti']}"
        )
        log.info(f"outlets: {context['task'].outlets}")

        # Update the outlets with the manifest
        _outlets = context["task"].outlets
        if not _outlets:
            log.info("on_success_callback no outlets")
            return
        outlet = context["task"].outlets[0]
        outlet_extra = context["outlet_events"][outlet].extra
        if len(outlet_extra.keys()) == 0:
            log.info("on_success_callback outlet_events no extra")
            outlet_extra = outlet.extra
        if "manifest" in outlet_extra:
            log.info("on_success_callback already has manifest")
            return

        # Upload the manifest for each connection and bucket
        assert "source" in outlet_extra
        outlet_manifest = defaultdict(list)
        log.info(f"outlet_extra: {outlet_extra}")
        source_dict = outlet_extra["source"]
        manifest_dict = outlet_extra.get("manifest", {})
        bucket = source_dict["defaults"]["bucket"]

        for output in outlet_extra["source"]["outputs"]:
            log.info(f"output: {output}")
            outlet_manifest[bucket].append(output["path"])

        hook = GCSHook()
        client = hook.get_conn()
        blobs = []

        for bucket_name in outlet_manifest:
            bucket_obj = client.bucket(bucket_name)
            for path in outlet_manifest[bucket_name]:
                manifest = [f"{path}/{_}" for _ in list_files_recursive(path)]
                log.info(f"listing {bucket_name} {path} {manifest}")
                for file_name in manifest:
                    object_name = "TESTING/" + file_name
                    log.info(f"Uploading {file_name} to {bucket_name} {object_name}")
                    hook.upload(
                        bucket_name=bucket_name,
                        object_name=object_name,
                        filename=file_name,
                    )
                    log.info(f"Uploaded {file_name} to {bucket} {object_name}")
                    blob = bucket_obj.get_blob(blob_name=object_name)
                    blobs.append(
                        blob_to_dict(
                            blob,
                            extra={
                                "bucket": bucket_name,
                                "gcp_conn_id": hook.gcp_conn_id,
                            },
                        )
                    )

        # Update the outlets with the manifest
        extra = {"manifest": blobs, "source": source_dict}
        context["outlet_events"][outlet].extra = extra
        log.info(f"Updated outlet {outlet} with extra: {extra}")

        # Clean up the temporary directory
        shutil.rmtree(cwd)
        log.info(f"Removed {cwd}")
    except Exception as e:
        log.error(f"Error in on_success_callback: {e}")
        context["ti"].xcom_push(key="error", value=str(e))
        context["ti"].set_state(State.FAILED)


def blob_to_dict(blob, extra: dict = {}) -> dict:
    """
    Convert a GCS blob to a dictionary.

    Args:
        blob: The GCS blob object.
        extra (dict): Additional data to include in the dictionary.

    Returns:
        dict: A dictionary representation of the blob.
    """
    return {
        "name": blob.name,
        "size": blob.size,
        "content_type": blob.content_type,
        "updated": blob.updated.isoformat(),
        "generation": blob.generation,
        "metageneration": blob.metageneration,
        "etag": blob.etag,
    } | extra


def make_tmp_dir(dag_id: str) -> str:
    """
    Create a temporary directory, /tmp + DAG ID using the convention of project-source-[ETL] e.g. /tmp/project-source.

    Args:
        dag_id (str): The ID of the DAG.

    Returns:
        str: The path to the temporary directory.
    """
    dag_id_parts = dag_id.split("-")
    tmp_dir = "/tmp/" + "-".join(dag_id_parts[:-1])
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir


def pre_execute(context):
    """
    Change to the temporary directory before executing the command, download inputs from GCS

    Args:
        context (dict): The context dictionary provided by Airflow.
    """
    log.info(context)
    tmp_dir = make_tmp_dir(context["dag"].dag_id)
    os.chdir(tmp_dir)
    context["tmp_dir"] = tmp_dir
    log.info(f"cd to {os.getcwd()}")

    inlets = context["task"].inlets
    if not inlets:
        log.info("pre_execute no inlets")
        return

    inlet = inlets[0]
    inlet_extra = context["inlet_events"][inlet][-1].extra
    log.info(f"pre_execute inlets {inlet_extra}")

    # Download the manifest for each connection and bucket
    inlet_manifest = defaultdict(defaultdict)
    for blob in inlet_extra["manifest"]:
        if blob["bucket"] not in inlet_manifest[blob["gcp_conn_id"]]:
            inlet_manifest[blob["gcp_conn_id"]][blob["bucket"]] = []
        inlet_manifest[blob["gcp_conn_id"]][blob["bucket"]].append(blob)

    for gcp_conn_id in inlet_manifest:
        hook = GCSHook(gcp_conn_id=gcp_conn_id)
        for bucket in inlet_manifest[gcp_conn_id]:
            for blob in inlet_manifest[gcp_conn_id][bucket]:
                file_name = os.path.join(tmp_dir, blob["name"])
                # Ensure the directory for file_name exists
                os.makedirs(os.path.dirname(file_name), exist_ok=True)
                # Download the data
                location = hook.download(
                    bucket_name=bucket, object_name=blob["name"], filename=file_name
                )
                log.info(
                    f"Downloaded {blob['name']} to {file_name} at location {location}"
                )
