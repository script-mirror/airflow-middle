import os
import datetime
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from middle.utils import sanitize_string, setup_logger
from middle.airflow import enviar_whatsapp_erro, enviar_whatsapp_sucesso
from ons_dados_abertos.tasks import (
    start_task, end_task,
    # roda_container,
)
from ons_dados_abertos.constants import TASKS
from middle.utils import Constants

constants = Constants()
airflow_logger = LoggingMixin().log

logger = setup_logger(external_logger=airflow_logger)
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2025, 8, 19),
}

@dag(
    default_args=default_args,
    dag_id='ons-dados-abertos', 
    start_date=datetime.datetime(2025, 1, 23), 
    schedule="20 9,14,17 * * *", 
    catchup=False,
    tags=['dados-abertos', 'ons'],
)
def dag_ons_dados_abertos():
    start = start_task()
    end = end_task()
    tasks = []

    for produto in TASKS:
        task_id = produto
        t = DockerOperator(
            task_id=task_id,
            docker_url="tcp://docker-proxy:2375",
            image="ons-dados-abertos",
            environment={
                "nome": "{{ task.task_id }}",
                "ano": "{{ logical_date.year }}",
                "git_username": os.getenv("git_username"),
                "git_token": os.getenv("git_token"),
            },
            auto_remove="force",
            xcom_all=False,
            on_failure_callback = enviar_whatsapp_erro,
            # on_success_callback = enviar_whatsapp_sucesso,
        )

        tasks.append(t)
    start >> tasks >> end       
        
        
dag_ons_dados_abertos = dag_ons_dados_abertos()
