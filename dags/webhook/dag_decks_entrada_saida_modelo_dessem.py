from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="DECKS_ENTRADA_SAIDA_MODELO_DESSEM",
    description="DAG dos Decks de Entrada e Saída do Modelo DESSEM",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['decks', 'entrada', 'saida', 'modelo', 'dessem'],
    render_template_as_native_obj=True,
)
def dag_decks_entrada_saida_modelo_dessem():
    """
    DAG dos Decks de Entrada e Saída do Modelo DESSEM.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_decks_entrada_saida_modelo_dessem = SSHOperator(
        task_id="tasks_decks_entrada_saida_modelo_dessem",
        command="echo 'Executando tarefas dos Decks de Entrada e Saída do Modelo DESSEM'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_decks_entrada_saida_modelo_dessem >> final    
    
dag_decks_entrada_saida_modelo_dessem = dag_decks_entrada_saida_modelo_dessem()