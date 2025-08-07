from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="DECKS_PREVISAO_GERACAO_EOLICA_SEMANAL_WEOLSM",
    description="DAG dos Decks de Previsão de Geração Eólica Semanal WEOLSM",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['decks', 'eolica', 'semanal', 'weolsm'],
    render_template_as_native_obj=True,
)
def dag_decks_previsao_geracao_eolica_semanal_weolsm():
    """
    DAG dos Decks de Previsão de Geração Eólica Semanal WEOLSM.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_decks_previsao_geracao_eolica_semanal_weolsm = SSHOperator(
        task_id="tasks_decks_previsao_geracao_eolica_semanal_weolsm",
        command="echo 'Executando tarefas dos Decks de Previsão de Geração Eólica Semanal WEOLSM'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_decks_previsao_geracao_eolica_semanal_weolsm >> final    
    
dag_decks_previsao_geracao_eolica_semanal_weolsm = dag_decks_previsao_geracao_eolica_semanal_weolsm()