from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="IPDO_INFORMATIVO_PRELIMINAR_DIARIO",
    description="DAG do produto IPDO (Informativo Preliminar Diário da Operação)",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['ipdo'],
    render_template_as_native_obj=True,
)
def dag_ipdo_informativo_preliminar_diario():
    """
    DAG do produto IPDO (Informativo Preliminar Diário da Operação).
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_ipdo = SSHOperator(
        task_id="tasks_ipdo",
        command="echo 'Executando tarefas do IPDO Informativo Preliminar Diário da Operação'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_ipdo >> final    
    
dag_ipdo_informativo_preliminar_diario = dag_ipdo_informativo_preliminar_diario()