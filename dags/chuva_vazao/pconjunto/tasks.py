from airflow.decorators import task
from middle.utils import (
    Constants,
    setup_logger,
)

logger = setup_logger()
constants = Constants()


@task
def start_task(**kwargs):
    return None

@task
def end_task(**kwargs):
    return None
