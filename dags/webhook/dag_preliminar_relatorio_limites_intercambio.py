from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="PRELIMINAR_RELATORIO_LIMITES_INTERCAMBIO",
    description="DAG do Preliminar Relatório de Limites de Intercâmbio",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['preliminar', 'relatorio', 'limites', 'intercambio'],
    render_template_as_native_obj=True,
)
def dag_preliminar_relatorio_limites_intercambio():
    """
    DAG do Preliminar Relatório de Limites de Intercâmbio.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_preliminar_relatorio_limites_intercambio = SSHOperator(
        task_id="tasks_preliminar_relatorio_limites_intercambio",
        command="echo 'Executando tarefas do Preliminar Relatório de Limites de Intercâmbio'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_preliminar_relatorio_limites_intercambio >> final    
    
dag_preliminar_relatorio_limites_intercambio = dag_preliminar_relatorio_limites_intercambio()