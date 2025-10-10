import datetime
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from middle.utils import setup_logger
from middle.airflow import enviar_whatsapp_erro, enviar_whatsapp_sucesso
from estudos.cvu.tasks import (
    start_task, end_task,
)
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
    dag_id='ccee-dados-abertos', 
    start_date=datetime.datetime(2025, 10, 9), 
    schedule="0/5 * * * *",
    catchup=False,
    tags=['ccee', 'cvu'],
)


def dag_check_cvu():
    start = start_task()
    end = end_task()
    
    task_produto = DockerOperator(
        task_id="cvu",
        docker_url="tcp://docker-proxy:2375",
        image="ccee-dados-abertos",
        environment={
            "nome": "{{ task.task_id }}",
        },
        auto_remove="force",
        xcom_all=False,
        on_failure_callback = enviar_whatsapp_erro,
        on_success_callback = enviar_whatsapp_sucesso,
        )
    
    start >> task_produto
    task_produto >> end

dag_check_cvu = dag_check_cvu()