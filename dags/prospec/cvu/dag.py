from airflow.decorators import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from prospec.cvu.tasks import (
    check_atualizacao,
    export_data_to_db,
    end_task,
    enviar_whatsapp_erro,
    update_cvu_dadger_decomp,
    update_cvu_clast_newave,
)

@dag(
    dag_id="API_CCEE_CVU",
    description="Consulta a api da ccee",
    start_date=datetime(2025, 8, 1),
    schedule="*/5 7-23 * * *",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": enviar_whatsapp_erro,
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

@dag(
    dag_id="UPDATE_CVU_DECKS",
    description="Atualiza os decks com CVU",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": enviar_whatsapp_erro,
    },
    tags=["cvu", "ccee", "decks"],
    render_template_as_native_obj=True,
)
def dag_update_decks_cvu():
    cvu_dadger_decomp_task = update_cvu_dadger_decomp(
        tipos_cvu="{{ dag_run.conf.get('external_params').get('fontes_to_search') }}",
        dt_atualizacao="{{ dag_run.conf.get('external_params').get('dt_atualizacao') }}",
        ids_to_modify="{{ dag_run.conf.get('ids_to_modify') }}"
    )

    cvu_clast_newave_task = update_cvu_clast_newave(
        tipos_cvu="{{ dag_run.conf.get('external_params').get('fontes_to_search') }}",
        dt_atualizacao="{{ dag_run.conf.get('external_params').get('dt_atualizacao') }}",
        ids_to_modify="{{ dag_run.conf.get('ids_to_modify') }}"
    )

    trigger_dag_prospec = TriggerDagRunOperator(
        task_id='trigger_dag_prospec_atualizacao',
        trigger_dag_id='PROSPEC-SENSIBILIDADE-AUTOMATICA',
        conf={'nome_estudo': "{{dag_run.conf.external_params.task_to_execute}}"},  
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",
    )
    
    cvu_dadger_decomp_task >> cvu_clast_newave_task >> trigger_dag_prospec

dag_update_decks_cvu = dag_update_decks_cvu()