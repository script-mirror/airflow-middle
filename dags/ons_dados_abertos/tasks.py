from airflow.decorators import task
from docker.types import Mount
import os
from middle.utils import (
    Constants,
    setup_logger,
)
host_env = os.path.expanduser("~/.env")
logger = setup_logger()
constants = Constants()

@task
def start_task(**kwargs):
    return None

@task
def end_task(**kwargs):
    return None


@task.docker(
    image="ons-dados-abertos",
    environment={
        "nome": "{{ task.task_id }}",
        "ano": "{{ logical_date.year }}",
    },
    mounts=[
        Mount(source=host_env, target="/root/.env", type="bind")
    ],
    auto_remove='force',
)
def roda_container(**kwargs):
    pass
