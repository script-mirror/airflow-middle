from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="DECKS_ENTRADA_PREVCARGA_DESSEM",
    description="DAG dos Decks de Entrada PREVCARGA DESSEM",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['decks', 'entrada', 'prevcarga', 'dessem'],
    render_template_as_native_obj=True,
)
def dag_decks_entrada_prevcarga_dessem():
    """
    DAG dos Decks de Entrada PREVCARGA DESSEM.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_decks_entrada_prevcarga_dessem = SSHOperator(
        task_id="tasks_decks_entrada_prevcarga_dessem",
        command="echo 'Executando tarefas dos Decks de Entrada PREVCARGA DESSEM'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_decks_entrada_prevcarga_dessem >> final    
    
dag_decks_entrada_prevcarga_dessem = dag_decks_entrada_prevcarga_dessem()