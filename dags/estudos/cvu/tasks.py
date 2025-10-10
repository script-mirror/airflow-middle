from airflow.sdk import task
from middle.utils import (
    Constants,
    setup_logger,
    sanitize_string,
)

logger = setup_logger()
constants = Constants()

@task.branch
def start_task(**kwargs):
    return ["cvu"]


@task
def end_task(**kwargs):
    return None
