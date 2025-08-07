from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from ..utils.sender_message import callback_whatsapp_erro_padrao


@dag(
    dag_id="RELATORIO_ACOMPANHAMENTO_HIDROLOGICO",
    description="DAG do Relatório de Acompanhamento Hidrológico",
    catchup=False,
    default_args={
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=['relatorio', 'acompanhamento', 'hidrologico'],
    render_template_as_native_obj=True,
)
def dag_relatorio_acompanhamento_hidrologico():
    """
    DAG do Relatório de Acompanhamento Hidrológico.
    """
    inicio = DummyOperator(task_id="inicio")
    tasks_relatorio_acompanhamento_hidrologico = SSHOperator(
        task_id="tasks_relatorio_acompanhamento_hidrologico",
        command="echo 'Executando tarefas do Relatório de Acompanhamento Hidrológico'",
        ssh_conn_id="ssh_default"
    )
    final = DummyOperator(task_id="final")
    
    inicio >> tasks_relatorio_acompanhamento_hidrologico >> final    
    
dag_relatorio_acompanhamento_hidrologico = dag_relatorio_acompanhamento_hidrologico()