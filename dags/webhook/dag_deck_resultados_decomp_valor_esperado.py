from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="DECK_RESULTADOS_DECOMP_VALOR_ESPERADO",
    description="DAG do Deck Resultados DECOMP Valor Esperado",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['deck', 'decomp', 'valor_esperado', 'resultados'],
    render_template_as_native_obj=True,
)
def dag_deck_resultados_decomp_valor_esperado():
    """
    DAG do Deck Resultados DECOMP Valor Esperado.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_deck_resultados_decomp_valor_esperado = SSHOperator(
        task_id="tasks_deck_resultados_decomp_valor_esperado",
        command="echo 'Executando tarefas do Deck Resultados DECOMP Valor Esperado'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_deck_resultados_decomp_valor_esperado >> final    
    
dag_deck_resultados_decomp_valor_esperado = dag_deck_resultados_decomp_valor_esperado()