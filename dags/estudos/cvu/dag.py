from airflow.decorators import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from estudos.cvu.tasks import (
    check_atualizacao,
    export_data_to_db,
    end_task,
)

@dag(
    dag_id="ccee-cvu-whatcher",
    description="Consulta a api da ccee",
    start_date=datetime(2025, 8, 1),
    schedule="*/5 7-23 * * *",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["cvu", "ccee"],
    render_template_as_native_obj=True,
)
def dag_check_cvu():
    t1 = check_atualizacao()
    t2 = export_data_to_db()
    t3 = TriggerDagRunOperator(
        task_id="trigger_prospec_updater",
        trigger_dag_id="PROSPEC_UPDATER",
        conf={"external_params": "{{ dag_run.conf }}"},
        wait_for_completion=False,
        trigger_rule="none_failed_min_one_success",
    )
    t4 = end_task()

    t1 >> t2 >> t3 >> t4
    t1 >> t4

dag_check_cvu = dag_check_cvu()
