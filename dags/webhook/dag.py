import datetime
from airflow.decorators import dag
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import BranchPythonOperator
from middle.utils import sanitize_string
from middle.airflow import enviar_whatsapp_erro, enviar_whatsapp_sucesso
from webhook.tasks import (
    start_task, end_task,
    executar_task,
)
from webhook.constants import PRODUTOS_SINTEGRE
from middle.utils import Constants

constants = Constants()

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
        task_produto = BranchPythonOperator(
            task_id=sanitize_string(
                product,
                space_char='_',
            ),
            trigger_rule="none_failed_min_one_success",
            python_callable = executar_task,
            on_failure_callback=enviar_whatsapp_erro,
            on_success_callback=enviar_whatsapp_sucesso,
        )
    
        start >> task_produto
        task_produto >> end

dag_webhook = dag_webhook()