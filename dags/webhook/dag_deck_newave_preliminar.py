from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="DECK_NEWAVE_PRELIMINAR",
    description="DAG do Deck NEWAVE Preliminar",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['deck', 'newave', 'preliminar'],
    render_template_as_native_obj=True,
)
def dag_deck_newave_preliminar():
    """
    DAG do Deck NEWAVE Preliminar.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_deck_newave_preliminar = SSHOperator(
        task_id="tasks_deck_newave_preliminar",
        command="echo 'Executando tarefas do Deck NEWAVE Preliminar'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_deck_newave_preliminar >> final    
    
dag_deck_newave_preliminar = dag_deck_newave_preliminar()