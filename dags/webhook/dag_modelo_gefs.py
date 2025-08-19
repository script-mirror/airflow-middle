from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="MODELO_GEFS",
    description="DAG do Modelo GEFS",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['modelo', 'gefs'],
    render_template_as_native_obj=True,
)
def dag_modelo_gefs():
    """
    DAG do Modelo GEFS.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_modelo_gefs = SSHOperator(
        task_id="tasks_modelo_gefs",
        command="echo 'Executando tarefas do Modelo GEFS'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_modelo_gefs >> final    
    
dag_modelo_gefs = dag_modelo_gefs()