from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="NOTAS_TECNICAS_MEDIO_PRAZO",
    description="DAG das Notas Técnicas de Médio Prazo",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['notas', 'tecnicas', 'medio_prazo'],
    render_template_as_native_obj=True,
)
def dag_notas_tecnicas_medio_prazo():
    """
    DAG das Notas Técnicas de Médio Prazo.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_notas_tecnicas_medio_prazo = SSHOperator(
        task_id="tasks_notas_tecnicas_medio_prazo",
        command="echo 'Executando tarefas das Notas Técnicas de Médio Prazo'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_notas_tecnicas_medio_prazo >> final    
    
dag_notas_tecnicas_medio_prazo = dag_notas_tecnicas_medio_prazo()