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
    params = kwargs.get('dag_run').conf
    product_name = params.get('nome')
    task_id = sanitize_string(product_name, space_char='_',)
    return [task_id]


@task
def end_task(**kwargs):
    return None
