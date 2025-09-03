from airflow.decorators import task
from docker.types import Mount
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


@task.docker(
    image="ons-dados-abertos",
    environment={
        "nome": "{{ task.task_id }}",
        "ano": "{{ logical_date.year }}",
    },
    mount_tmp_dir=False,
    mounts=[
        Mount(source="/home/admin/.env", target="$HOME/.env", type="bind")
    ],
)
def roda_container(**kwargs):
    pass
