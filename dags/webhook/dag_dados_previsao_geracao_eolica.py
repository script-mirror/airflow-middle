from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="DADOS_PREVISAO_GERACAO_EOLICA",
    description="DAG dos Dados de Previsão de Geração Eólica",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['eolica', 'geracao'],
    render_template_as_native_obj=True,
)
def dag_dados_previsao_geracao_eolica():
    """
    DAG dos Dados de Previsão de Geração Eólica.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_dados_previsao_geracao_eolica = SSHOperator(
        task_id="tasks_dados_previsao_geracao_eolica",
        command="echo 'Executando tarefas dos Dados de Previsão de Geração Eólica'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_dados_previsao_geracao_eolica >> final    
    
dag_dados_previsao_geracao_eolica = dag_dados_previsao_geracao_eolica()