import datetime
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from middle.utils import setup_logger
from middle.airflow import enviar_whatsapp_erro, enviar_whatsapp_sucesso
from chuva_vazao.pconjunto.tasks import (
    start_task, end_task,
    check_ecmwf, check_gefs, check_eta,
)
from middle.utils import Constants

constants = Constants()
airflow_logger = LoggingMixin().log

logger = setup_logger(external_logger=airflow_logger)
default_args = {
    'start_date': datetime.datetime(2025, 9, 23),
}

@dag(
    default_args=default_args,
    dag_id='pconjunto', 
    schedule=None, 
    catchup=False,
    tags=['chuva-vazao'],
)
def dag_pconjunto():
    start = start_task()
    end = end_task()
    ecmwf = check_ecmwf(data_execucao="{{ logical_date.to_date_string() }}")
    gefs = check_gefs(data_execucao="{{ logical_date.to_date_string() }}")
    eta = check_eta(data_execucao="{{ logical_date.to_date_string() }}")

    t = DockerOperator(
        task_id='run_pconjunto',
        docker_url="tcp://docker-proxy:2375",
        image="pconjunto:latest",
        environment={
            "data_rodada": "{{ logical_date.to_date_string() }}",
        },
        auto_remove="force",
        xcom_all=False,
        on_failure_callback = enviar_whatsapp_erro,
        on_success_callback = enviar_whatsapp_sucesso,
    )
    
    start >> [ecmwf, gefs, eta] >> t >> end       
        
        
dag_pconjunto = dag_pconjunto()
