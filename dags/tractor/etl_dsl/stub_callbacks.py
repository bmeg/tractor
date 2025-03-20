import logging

log = logging.getLogger(__name__)


def stub_pre_execute(context):
    """
    Log a message before the task execution.

    Args:
        context (dict): The context dictionary containing task instance and other metadata.
    """
    log.info(f"stub_pre_execute {context}")


def stub_on_success(context):
    """
    Log a message upon successful task execution.

    Args:
        context (dict): The context dictionary containing task instance and other metadata.
    """
    log.info(f"stub_on_success {context}")


def stub_on_failure(context):
    """
    Log a message upon task failure.

    Args:
        context (dict): The context dictionary containing task instance and other metadata.
    """
    log.info(f"stub_on_failure {context}")