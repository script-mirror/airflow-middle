from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.ssh.operators.ssh import SSHOperator
from estudos.updater.tasks import prepare_update_command
from middle.utils import Constants

constants = Constants()

default_args = {
    'execution_timeout': timedelta(hours=8),
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 28),
}

@dag(
    default_args=default_args,
    dag_id='prospec-sensibilidade-automatica', 
    start_date=datetime(2025, 1, 23), 
    schedule=None, 
    catchup=False,
    tags=['prospec'],
)
def dag_prospec_atualizacao():
    
    command = prepare_update_command()
    
    
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run',
        ssh_conn_id='ssh_master',  
        command=f"'source {constants.ATIVAR_ENV} {{ ti.xcom_pull(task_ids='prepare_update_command') }}'",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

    command >> run_prospec_on_host

# Instância da DAG
dag_prospec_atualizacao = dag_prospec_atualizacao()
