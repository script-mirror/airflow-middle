from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="DECKS_NEWAVE",
    description="DAG dos Decks NEWAVE",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['decks', 'newave'],
    render_template_as_native_obj=True,
)
def dag_decks_newave():
    """
    DAG dos Decks NEWAVE.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_decks_newave = SSHOperator(
        task_id="tasks_decks_newave",
        command="echo 'Executando tarefas dos Decks NEWAVE'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_decks_newave >> final    
    
dag_decks_newave = dag_decks_newave()