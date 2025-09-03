
from datetime import datetime, timedelta
from typing import Dict, Optional
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session
from airflow.utils.dag_parsing_context import DagParsingContext
from middle.utils import Constants

consts = Constants()

# Command templates
CMD_BASE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/main_roda_estudos.py "
CMD_BASE_SENS = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/gerar_sensibilidade.py "
CMD_BASE_NW = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_nw_ons_to_ccee.py "
CMD_BASE_DC = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_dc_ons_to_ccee.py "
CMD_UPDATE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/update_estudos/update_prospec.py "

# Define datasets for dependency-driven scheduling
DATASET_UPDATE = Dataset("prospec://atualizacao")
DATASET_NEWAVE = Dataset("prospec://newave")

# Default arguments for all DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=8),
}

@task(provide_session=True)
def check_dag_state(dag_id: str, execution_date: datetime, session=None) -> None:
    """Check if the DAG is paused or already running."""
    context = DagParsingContext.get_parsing_context()
    dag = context.dag_bag.get_dag(dag_id)
    
    if dag.is_paused:
        raise AirflowSkipException(f"DAG {dag_id} is paused. Skipping execution.")
    
    active_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.state == 'running',
        DagRun.execution_date != execution_date
    ).all()
    
    if active_runs:
        raise AirflowSkipException(f"DAG {dag_id} is already running. Skipping: {active_runs}")

@task
def build_dynamic_command(base_cmd: str, dag_run_conf: Optional[Dict] = None) -> str:
    """Build a command string from base command and dag_run.conf parameters."""
    if not dag_run_conf:
        dag_run_conf = {}
    command = base_cmd
    for key, value in dag_run_conf.items():
        if value is not None:
            if key == "list_email":
                command += f' "{key}" \'{value}\''
            else:
                command += f' "{key}" "{value}"'
    return command

@task
def build_update_command(dag_run_conf: Optional[Dict] = None) -> Dict[str, str]:
    """Build update command and produto for triggering PROSPEC_ATUALIZACAO."""
    if not dag_run_conf:
        dag_run_conf = {}
    produto = dag_run_conf.get('produto', '')
    command = CMD_UPDATE
    for key, value in dag_run_conf.items():
        if value is not None:
            if key == "list_email":
                command += f' "{key}" \'{value}\''
            else:
                command += f' "{key}" "{value}"'
    return {'command': command, 'produto': f'REVISAO-{produto}'}

@task
def build_sensitivity_command(dag_run_conf: Optional[Dict] = None) -> str:
    """Build command for sensitivity analysis with dynamic parameters."""
    if not dag_run_conf:
        dag_run_conf = {}
    return f"{CMD_BASE_SENS} \"{str(dag_run_conf)}\""

# DAG Definitions
# 1.00-ENVIAR-EMAIL-ESTUDOS
with DAG(
    dag_id='1.00-ENVIAR-EMAIL-ESTUDOS',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    cmd = build_dynamic_command(CMD_BASE)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=cmd,
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> cmd >> run_prospec

# 1.01-PROSPEC_PCONJUNTO_DEFINITIVO
with DAG(
    dag_id='1.01-PROSPEC_PCONJUNTO_DEFINITIVO',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs P.CONJ rodada Definitiva",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.02-PROSPEC_PCONJUNTO_PREL
with DAG(
    dag_id='1.02-PROSPEC_PCONJUNTO_PREL',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='0 7 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs P.CONJ rodada Preliminar",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.03-PROSPEC_1RV
with DAG(
    dag_id='1.03-PROSPEC_1RV',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='21 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs NEXT-RV rodada Preliminar",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.04-PROSPEC_EC_EXT
with DAG(
    dag_id='1.04-PROSPEC_EC_EXT',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='0 19 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs EC-EXT rodada Definitiva",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.05-PROSPEC_CENARIO_10
with DAG(
    dag_id='1.05-PROSPEC_CENARIO_10',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='42 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs CENARIOS rodada Preliminar",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.06-PROSPEC_CENARIO_11
with DAG(
    dag_id='1.06-PROSPEC_CENARIO_11',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='33 7 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs CENARIOS rodada Preliminar, cenario 11",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.07-PROSPEC_CHUVA_0
with DAG(
    dag_id='1.07-PROSPEC_CHUVA_0',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='0 8 * * 1',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs P.ZERO rodada Preliminar",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.08-PROSPEC_GRUPOS-ONS
with DAG(
    dag_id='1.08-PROSPEC_GRUPOS-ONS',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs ONS-GRUPOS rodada Preliminar",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.10-PROSPEC_GFS
with DAG(
    dag_id='1.10-PROSPEC_GFS',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    cmd = build_dynamic_command(f"{CMD_BASE}prevs PREVS_PLUVIA_GFS rvs 8 mapas GFS")
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=cmd,
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> cmd >> run_prospec

# 1.11-PROSPEC_ATUALIZACAO
with DAG(
    dag_id='1.11-PROSPEC_ATUALIZACAO',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=[DATASET_NEWAVE],  # Triggered by NEWAVE dataset
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    cmd = build_dynamic_command(f"{CMD_BASE}prevs UPDATE rodada Preliminar")
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=cmd,
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> cmd >> run_prospec

# 1.12-PROSPEC_CONSISTIDO
with DAG(
    dag_id='1.12-PROSPEC_CONSISTIDO',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='0 8 * * 1',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs CONSISTIDO rodada Preliminar",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.13-PROSPEC_PCONJUNTO_PREL_PRECIPITACAO
with DAG(
    dag_id='1.13-PROSPEC_PCONJUNTO_PREL_PRECIPITACAO',
    default_args=default_args,
    start_date=datetime(2024, 7, 30),
    schedule='0 7 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=f"{CMD_BASE}prevs P.APR rodada Preliminar",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_prospec

# 1.14-PROSPEC_RODAR_SENSIBILIDADE
with DAG(
    dag_id='1.14-PROSPEC_RODAR_SENSIBILIDADE',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    cmd = build_sensitivity_command()
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=cmd,
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> cmd >> run_prospec

# 1.16-DECOMP_ONS-TO-CCEE
with DAG(
    dag_id='1.16-DECOMP_ONS-TO-CCEE',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state(dag.dag_id)
    run_decomp = SSHOperator(
        task_id='run_decomp',
        ssh_conn_id='ssh_master',
        command=CMD_BASE_DC,
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    check >> run_decomp

# 1.17-NEWAVE_ONS-TO-CCEE
with DAG(
    dag_id='1.17-NEWAVE_ONS-TO-CCEE',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
    datasets=[DATASET_NEWAVE],  # Produces NEWAVE dataset
) as dag:
    check = check_dag_state(dag.dag_id)
    run_newave = SSHOperator(
        task_id='run_newave',
        ssh_conn_id='ssh_master',
        command=CMD_BASE_NW,
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    trigger_atualizacao = TriggerDagRunOperator(
        task_id='trigger_atualizacao',
        trigger_dag_id='1.11-PROSPEC_ATUALIZACAO',
        conf={"nome_estudo": "REVISAO-NEWAVE"},
        wait_for_completion=False,
    )
    check >> run_newave >> trigger_atualizacao

# 1.18-PROSPEC_UPDATE
with DAG(
    dag_id='1.18-PROSPEC_UPDATE',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
    datasets=[DATASET_UPDATE],  # Produces UPDATE dataset
) as dag:
    check = check_dag_state(dag.dag_id)
    cmd_and_produto = build_update_command()
    run_prospec = SSHOperator(
        task_id='run_prospec',
        ssh_conn_id='ssh_master',
        command=cmd_and_produto['command'],
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule='none_failed_min_one_success',
    )
    trigger_atualizacao = TriggerDagRunOperator(
        task_id='trigger_atualizacao',
        trigger_dag_id='1.11-PROSPEC_ATUALIZACAO',
        conf={"nome_estudo": cmd_and_produto['produto']},
        wait_for_completion=False,
    )
    check >> cmd_and_produto >> run_prospec >> trigger_atualizacao
