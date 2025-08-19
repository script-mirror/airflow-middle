from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="ARQUIVOS_MODELOS_PREVISAO_VAZOES_SEMANAIS_PMO",
    description="DAG dos Arquivos dos Modelos de Previsão de Vazões Semanais PMO",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['vazoes', 'pmo', 'semanais'],
    render_template_as_native_obj=True,
)
def dag_arquivos_modelos_previsao_vazoes_semanais_pmo():
    """
    DAG dos Arquivos dos Modelos de Previsão de Vazões Semanais PMO.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_arquivos_modelos_vazoes_semanais_pmo = SSHOperator(
        task_id="tasks_arquivos_modelos_vazoes_semanais_pmo",
        command="echo 'Executando tarefas dos Arquivos dos Modelos de Previsão de Vazões Semanais PMO'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_arquivos_modelos_vazoes_semanais_pmo >> final    
    
dag_arquivos_modelos_previsao_vazoes_semanais_pmo = dag_arquivos_modelos_previsao_vazoes_semanais_pmo()