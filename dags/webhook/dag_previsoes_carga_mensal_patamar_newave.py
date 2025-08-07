from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="PREVISOES_CARGA_MENSAL_PATAMAR_NEWAVE",
    description="DAG das Previsões de Carga Mensal por Patamar NEWAVE",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['previsoes', 'carga', 'mensal', 'patamar', 'newave'],
    render_template_as_native_obj=True,
)
def dag_previsoes_carga_mensal_patamar_newave():
    """
    DAG das Previsões de Carga Mensal por Patamar NEWAVE.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_previsoes_carga_mensal_patamar_newave = SSHOperator(
        task_id="tasks_previsoes_carga_mensal_patamar_newave",
        command="echo 'Executando tarefas das Previsões de Carga Mensal por Patamar NEWAVE'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_previsoes_carga_mensal_patamar_newave >> final    
    
dag_previsoes_carga_mensal_patamar_newave = dag_previsoes_carga_mensal_patamar_newave()