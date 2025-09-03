from datetime import datetime, timedelta
from typing import Dict, Optional
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.models.dagrun import DagRun
from airflow.models.dag import DagModel
from airflow.utils.session import provide_session
from airflow.utils.session import create_session

from middle.utils import Constants

consts = Constants()

CMD_BASE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/main_roda_estudos.py "
CMD_BASE_SENS = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/gerar_sensibilidade.py "
CMD_BASE_NW = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_nw_ons_to_ccee.py "
CMD_BASE_DC = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_dc_ons_to_ccee.py "
CMD_UPDATE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/update_estudos/update_prospec.py "

DATASET_UPDATE = Dataset("prospec://atualizacao")
DATASET_NEWAVE = Dataset("prospec://newave")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=8),
}

def check_dag_state(**context) -> None:
    """
    Verifica se a DAG está pausada ou já em execução.
    Compatível com Airflow 3 (sem @provide_session).
    """
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]

    with create_session() as session:
        dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
        if not dag:
            raise AirflowSkipException(f"DAG {dag_id} não encontrada no banco de dados.")

        if dag.is_paused:
            raise AirflowSkipException(f"DAG {dag_id} está pausada. Pulando execução.")

        active_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == "running",
            DagRun.execution_date != execution_date,
        ).all()

        if active_runs:
            raise AirflowSkipException(
                f"DAG {dag_id} já está em execução. Pulando: {active_runs}"
            )

@task
def build_dynamic_command(base_cmd: str, dag_run_conf: Optional[Dict] = None) -> str:
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
    if not dag_run_conf:
        dag_run_conf = {}
    
    command = f"{CMD_BASE_SENS} \"{str(dag_run_conf)}\""
    return command

with DAG(
    dag_id='1.00-ENVIAR-EMAIL-ESTUDOS',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.01-PROSPEC_PCONJUNTO_DEFINITIVO',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.02-PROSPEC_PCONJUNTO_PREL',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='0 7 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.03-PROSPEC_1RV',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='21 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.04-PROSPEC_EC_EXT',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='0 19 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.05-PROSPEC_CENARIO_10',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='42 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.06-PROSPEC_CENARIO_11',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='33 7 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.07-PROSPEC_CHUVA_0',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='0 8 * * 1',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.08-PROSPEC_GRUPOS-ONS',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.10-PROSPEC_GFS',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.11-PROSPEC_ATUALIZACAO',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=[DATASET_NEWAVE, DATASET_UPDATE],
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.12-PROSPEC_CONSISTIDO',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule='0 8 * * 1',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.13-PROSPEC_PCONJUNTO_PREL_PRECIPITACAO',
    default_args=default_args,
    start_date=datetime(2024, 7, 30),
    schedule='0 7 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.14-PROSPEC_RODAR_SENSIBILIDADE',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.16-DECOMP_ONS-TO-CCEE',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.17-NEWAVE_ONS-TO-CCEE',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
    datasets=[DATASET_NEWAVE],
) as dag:
    check = check_dag_state()
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

with DAG(
    dag_id='1.18-PROSPEC_UPDATE',
    default_args=default_args,
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
    datasets=[DATASET_UPDATE],
) as dag:
    check = check_dag_state()
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
