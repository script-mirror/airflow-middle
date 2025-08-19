from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="MODELO_ECMWF",
    description="DAG do Modelo ECMWF",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['modelo', 'ecmwf'],
    render_template_as_native_obj=True,
)
def dag_modelo_ecmwf():
    """
    DAG do Modelo ECMWF.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_modelo_ecmwf = SSHOperator(
        task_id="tasks_modelo_ecmwf",
        command="echo 'Executando tarefas do Modelo ECMWF'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_modelo_ecmwf >> final    
    
dag_modelo_ecmwf = dag_modelo_ecmwf()