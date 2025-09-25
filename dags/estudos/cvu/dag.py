from airflow.decorators import dag
import datetime
from middle.utils import Constants, setup_logger

from estudos.cvu.tasks import (
    check_atualizacao,
    export_data_to_db,
    trigger_prospec_updater,
    end_task,
)

logger = setup_logger()
constants = Constants()


@dag(
    dag_id="ccee-cvu-whatcher",
    description="Consulta a api da ccee",
    start_date=datetime.datetime(2025, 8, 1),
    schedule="*/5 7-23 * * *",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=1),
    },
    tags=["cvu", "ccee"],
    render_template_as_native_obj=True,
)


def dag_check_cvu():

    t1 = check_atualizacao()
    t2 = export_data_to_db()
    t3 = trigger_prospec_updater()
    t4 = end_task()

    t1 >> t2 >> t3 >> t4
    t1 >> t4

dag_check_cvu = dag_check_cvu()