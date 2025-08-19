from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="MODELO_ETA",
    description="DAG do Modelo ETA",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['modelo', 'eta'],
    render_template_as_native_obj=True,
)
def dag_modelo_eta():
    """
    DAG do Modelo ETA.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_modelo_eta = SSHOperator(
        task_id="tasks_modelo_eta",
        command="echo 'Executando tarefas do Modelo ETA'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_modelo_eta >> final    
    
dag_modelo_eta = dag_modelo_eta()