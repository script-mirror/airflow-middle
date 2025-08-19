from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="RELATORIO_LIMITES_INTERCAMBIO_MODELO_DECOMP",
    description="DAG do Relatório de Limites de Intercâmbio do Modelo DECOMP",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['relatorio', 'limites', 'intercambio', 'modelo', 'decomp'],
    render_template_as_native_obj=True,
)
def dag_relatorio_limites_intercambio_modelo_decomp():
    """
    DAG do Relatório de Limites de Intercâmbio do Modelo DECOMP.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_relatorio_limites_intercambio_modelo_decomp = SSHOperator(
        task_id="tasks_relatorio_limites_intercambio_modelo_decomp",
        command="echo 'Executando tarefas do Relatório de Limites de Intercâmbio do Modelo DECOMP'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_relatorio_limites_intercambio_modelo_decomp >> final    
    
dag_relatorio_limites_intercambio_modelo_decomp = dag_relatorio_limites_intercambio_modelo_decomp()