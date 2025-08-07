from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="RESULTADOS_PRELIMINARES_NAO_CONSISTIDOS_VAZOES_SEMANAIS_PMO",
    description="DAG dos Resultados Preliminares Não Consistidos de Vazões Semanais PMO",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['resultados', 'preliminares', 'nao_consistidos', 'vazoes', 'semanais', 'pmo'],
    render_template_as_native_obj=True,
)
def dag_resultados_preliminares_nao_consistidos_vazoes_semanais_pmo():
    """
    DAG dos Resultados Preliminares Não Consistidos de Vazões Semanais PMO.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_resultados_preliminares_nao_consistidos_vazoes_semanais_pmo = SSHOperator(
        task_id="tasks_resultados_preliminares_nao_consistidos_vazoes_semanais_pmo",
        command="echo 'Executando tarefas dos Resultados Preliminares Não Consistidos de Vazões Semanais PMO'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_resultados_preliminares_nao_consistidos_vazoes_semanais_pmo >> final    
    
dag_resultados_preliminares_nao_consistidos_vazoes_semanais_pmo = dag_resultados_preliminares_nao_consistidos_vazoes_semanais_pmo()