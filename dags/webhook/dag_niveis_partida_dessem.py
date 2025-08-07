from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="NIVEIS_PARTIDA_DESSEM",
    description="DAG dos Níveis de Partida DESSEM",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['niveis', 'partida', 'dessem'],
    render_template_as_native_obj=True,
)
def dag_niveis_partida_dessem():
    """
    DAG dos Níveis de Partida DESSEM.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_niveis_partida_dessem = SSHOperator(
        task_id="tasks_niveis_partida_dessem",
        command="echo 'Executando tarefas dos Níveis de Partida DESSEM'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_niveis_partida_dessem >> final    
    
dag_niveis_partida_dessem = dag_niveis_partida_dessem()