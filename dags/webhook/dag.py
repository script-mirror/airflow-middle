import datetime
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from middle.utils import sanitize_string, setup_logger
from middle.airflow import enviar_whatsapp_erro, enviar_whatsapp_sucesso
from webhook.tasks import (
    start_task, end_task,
)
from webhook.constants import PRODUTOS_SINTEGRE
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
    dag_id='webhook-sintegre', 
    start_date=datetime.datetime(2025, 1, 23), 
    schedule=None, 
    catchup=False,
    tags=['webhook', 'ons'],
)
def dag_webhook():
    start = start_task()
    end = end_task()
    
    for product in PRODUTOS_SINTEGRE:
        task_produto = DockerOperator(
            task_id=sanitize_string(
                product,
                space_char='_',
            ),
            docker_url="tcp://docker-proxy:2375",
            image="task-webhook-ons",
            command="'{{ dag_run.conf }}'",
            auto_remove="force",
            xcom_all=False,
            on_failure_callback = enviar_whatsapp_erro,
            on_success_callback = enviar_whatsapp_sucesso,
        )
    
        start >> task_produto
        task_produto >> end

dag_webhook = dag_webhook()