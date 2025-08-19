from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="CARGA_PATAMAR_DECOMP",
    description="DAG da Carga Patamar DECOMP",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['carga', 'patamar', 'decomp'],
    render_template_as_native_obj=True,
)
def dag_carga_patamar_decomp():
    """
    DAG da Carga Patamar DECOMP.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_carga_patamar_decomp = SSHOperator(
        task_id="tasks_carga_patamar_decomp",
        command="echo 'Executando tarefas da Carga Patamar DECOMP'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_carga_patamar_decomp >> final    
    
dag_carga_patamar_decomp = dag_carga_patamar_decomp()