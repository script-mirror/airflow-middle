from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="ARQUIVOS_PREVISAO_CARGA_DESSEM",
    description="DAG dos Arquivos de Previsão de Carga DESSEM",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['carga', 'dessem'],
    render_template_as_native_obj=True,
)
def dag_arquivos_previsao_carga_dessem():
    """
    DAG dos Arquivos de Previsão de Carga DESSEM.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_arquivos_previsao_carga_dessem = SSHOperator(
        task_id="tasks_arquivos_previsao_carga_dessem",
        command="echo 'Executando tarefas dos Arquivos de Previsão de Carga DESSEM'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_arquivos_previsao_carga_dessem >> final    
    
dag_arquivos_previsao_carga_dessem = dag_arquivos_previsao_carga_dessem()