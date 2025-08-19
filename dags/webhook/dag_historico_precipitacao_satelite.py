from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="HISTORICO_PRECIPITACAO_SATELITE",
    description="DAG do Histórico de Precipitação por Satélite",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['historico', 'precipitacao', 'satelite'],
    render_template_as_native_obj=True,
)
def dag_historico_precipitacao_satelite():
    """
    DAG do Histórico de Precipitação por Satélite.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_historico_precipitacao_satelite = SSHOperator(
        task_id="tasks_historico_precipitacao_satelite",
        command="echo 'Executando tarefas do Histórico de Precipitação por Satélite'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_historico_precipitacao_satelite >> final    
    
dag_historico_precipitacao_satelite = dag_historico_precipitacao_satelite()