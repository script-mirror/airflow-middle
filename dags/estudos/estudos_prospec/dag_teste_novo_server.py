from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.session import provide_session
from airflow.models import DagRun
from airflow.models.dag import DagModel
from middle.utils import Constants

consts = Constants()

# Base commands (stripped of SSH prefix for SSHOperator)
CMD_BASE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/main_roda_estudos.py"
CMD_BASE_SENS = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/gerar_sensibilidade.py"
CMD_BASE_NW = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_nw_ons_to_ccee.py"
CMD_BASE_DC = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_dc_ons_to_ccee.py"
CMD_UPDATE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/update_estudos/update_prospec.py"

default_args = {
    'owner': 'airflow',
}



# DAG 4: 1.03-PROSPEC_1RV
@dag(
    dag_id='PROSPEC_TESTE',
    start_date=datetime(2024, 4, 28),
    schedule="21 06 * * *",
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_1rv():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_1rv',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs NEXT-RV rodada Preliminar",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
        
    )

prospec_1rv()

