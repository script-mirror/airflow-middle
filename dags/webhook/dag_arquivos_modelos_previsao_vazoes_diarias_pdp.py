from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="ARQUIVOS_MODELOS_PREVISAO_VAZOES_DIARIAS_PDP",
    description="DAG dos Arquivos dos Modelos de Previsão de Vazões Diárias PDP",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['vazoes', 'pdp', 'diarias'],
    render_template_as_native_obj=True,
)
def dag_arquivos_modelos_previsao_vazoes_diarias_pdp():
    """
    DAG dos Arquivos dos Modelos de Previsão de Vazões Diárias PDP.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_arquivos_modelos_vazoes_diarias_pdp = SSHOperator(
        task_id="tasks_arquivos_modelos_vazoes_diarias_pdp",
        command="echo 'Executando tarefas dos Arquivos dos Modelos de Previsão de Vazões Diárias PDP'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_arquivos_modelos_vazoes_diarias_pdp >> final    
    
dag_arquivos_modelos_previsao_vazoes_diarias_pdp = dag_arquivos_modelos_previsao_vazoes_diarias_pdp()