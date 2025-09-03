from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.session import provide_session
from airflow.models import DagRun
from airflow.models.dag import DagModel  # Updated import for Airflow 3.0
from middle.utils import Constants
from airflow.providers.ssh.hooks.ssh import SSHHook
consts = Constants()

# Comandos base
CMD_BASE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/main_roda_estudos.py "
CMD_BASE_SENS = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/gerar_sensibilidade.py "
CMD_BASE_NW = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_nw_ons_to_ccee.py "
CMD_BASE_DC = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_dc_ons_to_ccee.py "
CMD_UPDATE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/update_estudos/update_prospec.py "

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
    @task
    def run_python_script_with_dynamic_params(**kwargs):
        params = kwargs.get('params', {})
        conteudo = ' '.join(f'"{k}" \'{v}\'' if k == "list_email" else f'"{k}" "{v}"' for k, v in params.items())
        command = CMD_BASE + conteudo
        print(command)
        return {'command': command}

    run_script_task = run_python_script_with_dynamic_params()
    run_prospec_on_host = SSHOperator(
        task_id='run',
        ssh_conn_id='ssh_master',
        command="{{ task_instance.xcom_pull(task_ids='run_python_script_with_dynamic_params')['command'] }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

    run_script_task >> run_prospec_on_host

enviar_email_estudos()

# DAG 2: 1.01-PROSPEC_PCONJUNTO_DEFINITIVO
@dag(
    dag_id='1.01-PROSPEC_PCONJUNTO_DEFINITIVO',
    start_date=datetime(2024, 4, 28),
    schedule=None,
    catchup=False,
    tags=['Prospec'],
    default_args=default_args,
)
def prospec_pconjunto_definitivo():
    @task
    @provide_session
    def check_if_dag_is_running(session=None, **kwargs):
        dag_id = kwargs['dag'].dag_id
        execution_date = kwargs['execution_date']
        active_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == 'running',
            DagRun.execution_date != execution_date
        ).all()
        if active_runs:
            raise AirflowSkipException(f"DAG {dag_id} já está em execução. Pulando execução {active_runs}.")

    check_running = check_if_dag_is_running()
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_on_host',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs P.CONJ rodada Definitiva",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

    check_running >> run_prospec_on_host

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
        command=CMD_BASE + "prevs P.CONJ rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

prospec_pconjunto_prel()

ssh_hook = SSHHook(
    ssh_conn_id="ssh_master",
    key_file="/opt/airflow/config/chave-middle.pem",
    no_host_key_check=True,
)
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
        ssh_hook=ssh_hook,
        command=CMD_BASE + "prevs NEXT-RV rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
        command=CMD_BASE + "prevs EC-EXT rodada Definitiva",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
        command=CMD_BASE + "prevs CENARIOS rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
        command=CMD_BASE + "prevs CENARIOS rodada Preliminar, cenario 11",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
        command=CMD_BASE + "prevs P.ZERO rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
    @task.branch
    @provide_session
    def check_dag_state(session=None, **kwargs):
        dag_id = '1.08-PROSPEC_GRUPOS-ONS'
        dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
        return 'skip_task' if dag.is_paused else 'run_decomp_ons_grupos'

    @task
    def skip_task():
        print("A DAG está pausada, a tarefa não será executada.")

    check_dag_state_task = check_dag_state()
    skip = skip_task()
    run_decomp_ons_grupos = SSHOperator(
        task_id='run_decomp_ons_grupos',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs ONS-GRUPOS rodada Preliminar",
        conn_timeout=None,
        cmd_timeout=None,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

    check_dag_state_task >> [run_decomp_ons_grupos, skip]

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
    @task
    def run_python_gfs(**kwargs):
        params = kwargs.get('params', {})
        command = CMD_BASE + "prevs PREVS_PLUVIA_GFS rvs 8 mapas GFS"
        for key, value in params.items():
            if value is not None:
                command += f" {key} '{value}'"
        print(command)
        return {'command': command}

    run_script_task = run_python_gfs()
    run_prospec_on_host = SSHOperator(
        task_id='run',
        ssh_conn_id='ssh_master',
        command="{{ task_instance.xcom_pull(task_ids='run_python_gfs')['command'] }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

    run_script_task >> run_prospec_on_host

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
    @task
    def run_python_update_with_dynamic_params(**kwargs):
        params = kwargs.get('params', {})
        command = CMD_BASE + "prevs UPDATE rodada Preliminar"
        for key, value in params.items():
            if value is not None:
                command += f" {key} '{value}'"
        print(command)
        return {'command': command}

    run_script_task = run_python_update_with_dynamic_params()
    run_prospec_on_host = SSHOperator(
        task_id='run',
        ssh_conn_id='ssh_master',
        command="{{ task_instance.xcom_pull(task_ids='run_python_update_with_dynamic_params')['command'] }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

    run_script_task >> run_prospec_on_host

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
        command=CMD_BASE + "prevs CONSISTIDO rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
        command=CMD_BASE + "prevs P.APR rodada Preliminar",
        conn_timeout=None,
        cmd_timeout=None,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
    @task
    def run_sensibilidades_params(**kwargs):
        params = kwargs.get('params', {})
        command = CMD_BASE_SENS + " ".join(f"{k} '{v}'" for k, v in params.items() if v is not None)
        print(command)
        return {'command': command}

    run_script_task = run_sensibilidades_params()
    run_prospec_on_host = SSHOperator(
        task_id='run',
        ssh_conn_id='ssh_master',
        command="{{ task_instance.xcom_pull(task_ids='run_sensibilidades_params')['command'] }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

    run_script_task >> run_prospec_on_host

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
        command=CMD_BASE_DC,
        conn_timeout=None,
        cmd_timeout=None,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
        command=CMD_BASE_NW,
        conn_timeout=None,
        cmd_timeout=None,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
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
@dag(
    dag_id='1.18-PROSPEC_UPDATE',
    start_date=datetime(2025, 1, 23),
    schedule=None,
    catchup=False,
    tags=['Prospec'],
    max_active_runs=1,
    default_args=default_args,
)
def prospec_update():
    @task
    def run_prospec_update(**kwargs):
        params = kwargs.get('params', {})
        produto = params.get('produto', '')
        conteudo = ' '.join(f'"{k}" \'{v}\'' if k == "list_email" else f'"{k}" "{v}"' for k, v in params.items())
        command = CMD_UPDATE + conteudo
        print(command)
        return {'command': command, 'produto': f'REVISAO-{produto}'}

    run_script_task = run_prospec_update()
    run_prospec_on_host = SSHOperator(
        task_id='run',
        ssh_conn_id='ssh_master',
        command="{{ task_instance.xcom_pull(task_ids='run_prospec_update')['command'] }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

    trigger_atualizacao = TriggerDagRunOperator(
        task_id='trigger_atualizacao',
        trigger_dag_id='1.11-PROSPEC_ATUALIZACAO',
        conf={"nome_estudo": "{{ task_instance.xcom_pull(task_ids='run_prospec_update')['produto'] }}"},
        wait_for_completion=False,
    )

    run_script_task >> run_prospec_on_host >> trigger_atualizacao

prospec_update()