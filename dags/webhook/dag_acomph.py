from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="ACOMPH",
    description="DAG do produto ACOMPH",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['acomph'],
    render_template_as_native_obj=True,
)
def dag_acomph():
    """
    DAG do produto ACOMPH.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_acomph = SSHOperator(
        task_id="tasks_acomph",
        command="echo 'Executando tarefas do ACOMPH'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_acomph >> final    
    
dag_acomph = dag_acomph()