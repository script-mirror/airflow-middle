from airflow.decorators import task
from docker.types import Mount
import os
from middle.utils import (
    Constants,
    setup_logger,
)

logger = setup_logger()
constants = Constants()

git_username = os.getenv("git_username")
git_token = os.getenv("git_token")

if not git_username or not git_token:
    logger.warning("Variáveis git_username ou git_token não encontradas no ambiente")


@task
def start_task(**kwargs):
    return None

@task
def end_task(**kwargs):
    return None


@task.docker(
    docker_url="tcp://docker-proxy:2375",
    image="ons-dados-abertos",
    environment={
        "nome": "{{ task.task_id }}",
        "ano": "{{ logical_date.year }}",
        "git_username": os.getenv("git_username"),
        "git_token": os.getenv("git_token"),
    },
    auto_remove='force',
)
def roda_container(**kwargs):
    return True
