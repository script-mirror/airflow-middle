from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="RDH",
    description="DAG do produto RDH",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['rdh'],
    render_template_as_native_obj=True,
)
def dag_rdh():
    """
    DAG do produto RDH.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_rdh = SSHOperator(
        task_id="tasks_rdh",
        command="echo 'Executando tarefas do RDH'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_rdh >> final    
    
dag_rdh = dag_rdh()