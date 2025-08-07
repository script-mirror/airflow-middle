from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="DADVAZ_PREVISAO_VAZOES_DIARIAS_PDP",
    description="DAG do DADVAZ Previsão de Vazões Diárias PDP",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['dadvaz', 'vazoes', 'pdp', 'diarias'],
    render_template_as_native_obj=True,
)
def dag_dadvaz_previsao_vazoes_diarias_pdp():
    """
    DAG do DADVAZ Previsão de Vazões Diárias PDP.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_dadvaz_previsao_vazoes_diarias_pdp = SSHOperator(
        task_id="tasks_dadvaz_previsao_vazoes_diarias_pdp",
        command="echo 'Executando tarefas do DADVAZ Previsão de Vazões Diárias PDP'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_dadvaz_previsao_vazoes_diarias_pdp >> final    
    
dag_dadvaz_previsao_vazoes_diarias_pdp = dag_dadvaz_previsao_vazoes_diarias_pdp()