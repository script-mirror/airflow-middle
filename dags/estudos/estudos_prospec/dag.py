from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.exceptions import AirflowException
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from middle.utils import Constants

consts = Constants()

# Base commands (stripped of SSH prefix for SSHOperator)
CMD_BASE = f"{consts.ATIVAR_ENV} python -u {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/main_roda_estudos.py"
CMD_BASE_SENS = f"{consts.ATIVAR_ENV} python -u {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/gerar_sensibilidade.py"
CMD_BASE_NW = f"{consts.ATIVAR_ENV} python -u {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_nw_ons_to_ccee.py"
CMD_BASE_DC = f"{consts.ATIVAR_ENV} python -u {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_dc_ons_to_ccee.py"
CMD_UPDATE = f"{consts.ATIVAR_ENV} python -u {consts.PATH_PROJETOS}/estudos-middle/update_estudos/update_prospec.py"

default_args = {
    'owner': 'airflow',
}

# DAG 1: 1.00-ENVIAR-EMAIL-ESTUDOS
@dag(
    dag_id='1.00-ENVIAR-EMAIL-ESTUDOS',
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def enviar_email_estudos():
    run_prospec_on_host = SSHOperator(
        task_id='run_email_estudos',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

enviar_email_estudos()

# DAG 2: 1.01-PROSPEC_PCONJUNTO_DEFINITIVO
@dag(
    dag_id='1.01-PROSPEC_PCONJUNTO_DEFINITIVO',
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,  # Added to prevent concurrent runs
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_pconjunto_definitivo():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_on_host',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs P.CONJ rodada Definitiva",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_pconjunto_definitivo()

# DAG 3: 1.02-PROSPEC_PCONJUNTO_PREL
@dag(
    dag_id='1.02-PROSPEC_PCONJUNTO_PREL',
    start_date=datetime(2024, 4, 28),
    schedule='02 07 * * *',
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_pconjunto_prel():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_pconj_prel',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs P.CONJ rodada Preliminar",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_pconjunto_prel()

# DAG 4: 1.03-PROSPEC_1RV
@dag(
    dag_id='1.03-PROSPEC_1RV',
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

# DAG 5: 1.04-PROSPEC_EC_EXT
@dag(
    dag_id='1.04-PROSPEC_EC_EXT',
    start_date=datetime(2024, 4, 28),
    schedule="00 19 * * *",
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_ec_ext():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_ec_ext',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs EC-EXT rodada Definitiva",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_ec_ext()

# DAG 6: 1.05-PROSPEC_CENARIO_10
@dag(
    dag_id='1.05-PROSPEC_CENARIO_10',
    start_date=datetime(2024, 4, 28),
    schedule='42 6 * * *',
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_cenario_10():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_cenario_10',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs CENARIOS rodada Preliminar",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_cenario_10()

# DAG 7: 1.06-PROSPEC_CENARIO_11
@dag(
    dag_id='1.06-PROSPEC_CENARIO_11',
    start_date=datetime(2024, 4, 28),
    schedule='33 7 * * 1-5',
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_cenario_11():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_cenario_11',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs CENARIOS rodada Preliminar, cenario 11",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_cenario_11()

# DAG 8: 1.07-PROSPEC_CHUVA_0
@dag(
    dag_id='1.07-PROSPEC_CHUVA_0',
    start_date=datetime(2024, 4, 28),
    schedule='00 8 * * 1',
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_chuva_0():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_chuva0',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs P.ZERO rodada Preliminar",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_chuva_0()

# DAG 9: 1.08-PROSPEC_GRUPOS-ONS
@dag(
    dag_id='1.08-PROSPEC_GRUPOS-ONS',
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_grupos_ons():
    run_decomp_ons_grupos = SSHOperator(
        task_id='run_decomp_ons_grupos',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs ONS-GRUPOS rodada Preliminar",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_grupos_ons()

# DAG 10: 1.10-PROSPEC_GFS
@dag(
    dag_id='1.10-PROSPEC_GFS',
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_gfs():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_gfs',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs PREVS_PLUVIA_GFS rvs 8 mapas GFS",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_gfs()

# DAG 11: 1.11-PROSPEC_ATUALIZACAO
@dag(
    dag_id='1.11-PROSPEC_ATUALIZACAO',
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_atualizacao():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_atualizacao',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs UPDATE rodada Preliminar",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_atualizacao()

# DAG 12: 1.12-PROSPEC_CONSISTIDO
@dag(
    dag_id='1.12-PROSPEC_CONSISTIDO',
    start_date=datetime(2024, 4, 28),
    schedule='00 8 * * 1',
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_consistido():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_consistido',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs CONSISTIDO rodada Preliminar",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_consistido()

# DAG 13: 1.13-PROSPEC_PCONJUNTO_PREL_PRECIPITACAO
@dag(
    dag_id='1.13-PROSPEC_PCONJUNTO_PREL_PRECIPITACAO',
    start_date=datetime(2024, 7, 30),
    schedule='00 07 * * 1-5',
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_pconjunto_prel_precipitacao():
    run_pconjunto_prel_precipitacao = SSHOperator(
        task_id='run_pconjunto_prel_precipitacao',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE} prevs P.APR rodada Preliminar",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_pconjunto_prel_precipitacao()

# DAG 14: 1.14-PROSPEC_RODAR_SENSIBILIDADE
@dag(
    dag_id='1.14-PROSPEC_RODAR_SENSIBILIDADE',
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_rodar_sensibilidade():
    run_prospec_on_host = SSHOperator(
        task_id='run_sensibilidade',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE_SENS}",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

prospec_rodar_sensibilidade()

# DAG 15: 1.16-DECOMP_ONS-TO-CCEE
@dag(
    dag_id='1.16-DECOMP_ONS-TO-CCEE',
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def decomp_ons_to_ccee():
    run_decomp_on_host = SSHOperator(
        task_id='run_decomp',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE_DC}",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

decomp_ons_to_ccee()

# DAG 16: 1.17-NEWAVE_ONS-TO-CCEE
@dag(
    dag_id='1.17-NEWAVE_ONS-TO-CCEE',
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def newave_ons_to_ccee():
    run_nw_on_host = SSHOperator(
        task_id='run_newave',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE_NW}",
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

    trigger_atualizacao = TriggerDagRunOperator(
        task_id='trigger_atualizacao',
        trigger_dag_id='1.11-PROSPEC_ATUALIZACAO',
        conf={"nome_estudo": 'REVISAO-NEWAVE'},
        wait_for_completion=False,
    )

    run_nw_on_host >> trigger_atualizacao

newave_ons_to_ccee()

# DAG 17: 1.18-PROSPEC_UPDATE
def run_prospec_update(**kwargs):
    params = kwargs.get('params', {})
    produto = params.get('produto')
    if not produto:
        raise AirflowException("Parâmetro 'produto' é obrigatório.")

    command = f"{CMD_UPDATE} 'REVISAO-{produto}"

    return command

@dag(
    dag_id='1.18-PROSPEC_UPDATE',
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
    default_args=default_args,
)

def prospec_update():
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_update',
        ssh_conn_id='ssh_master',
        command=run_prospec_update(),
        trigger_rule="none_failed_min_one_success",
        conn_timeout=None,
        cmd_timeout=None,
    )

    trigger_atualizacao = TriggerDagRunOperator(
        task_id='trigger_atualizacao',
        trigger_dag_id='1.11-PROSPEC_ATUALIZACAO',
        conf={"nome_estudo": "REVISAO"},
        wait_for_completion=False,
    )

    run_prospec_on_host >> trigger_atualizacao

prospec_update()