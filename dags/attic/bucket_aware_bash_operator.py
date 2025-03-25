import logging
from typing import Any

from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)


class BucketAwareBashOperator(BashOperator):
    """Setup environment."""
    def execute(self, context) -> Any:
        return super().execute(context)
