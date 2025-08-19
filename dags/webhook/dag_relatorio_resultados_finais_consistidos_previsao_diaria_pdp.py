from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="RELATORIO_RESULTADOS_FINAIS_CONSISTIDOS_PREVISAO_DIARIA_PDP",
    description="DAG do Relatório de Resultados Finais Consistidos de Previsão Diária PDP",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['relatorio', 'resultados', 'finais', 'consistidos', 'previsao', 'diaria', 'pdp'],
    render_template_as_native_obj=True,
)
def dag_relatorio_resultados_finais_consistidos_previsao_diaria_pdp():
    """
    DAG do Relatório de Resultados Finais Consistidos de Previsão Diária PDP.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_relatorio_resultados_finais_consistidos_previsao_diaria_pdp = SSHOperator(
        task_id="tasks_relatorio_resultados_finais_consistidos_previsao_diaria_pdp",
        command="echo 'Executando tarefas do Relatório de Resultados Finais Consistidos de Previsão Diária PDP'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_relatorio_resultados_finais_consistidos_previsao_diaria_pdp >> final    
    
dag_relatorio_resultados_finais_consistidos_previsao_diaria_pdp = dag_relatorio_resultados_finais_consistidos_previsao_diaria_pdp()