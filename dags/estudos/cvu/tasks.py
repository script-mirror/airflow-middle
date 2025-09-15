import requests
import datetime
from airflow.decorators import task
from airflow.operators.python import get_current_context
from middle.utils import Constants, get_auth_header, setup_logger
from middle.airflow import trigger_dag

logger = setup_logger()
constants = Constants()

def _get_cvu_mapping():
    res = requests.get(
        f'{constants.BASE_URL}/estudos-middle/api/ccee/cvu-mapping',
        headers=get_auth_header()
    )
    if res.status_code == 200:
        return res.json()
    raise Exception(f"Erro ao obter mapeamento CVU: {res.status_code} - {res.text}")

def _get_data_atualizacao(tipo_cvu: str):
    res = requests.get(
        f'{constants.BASE_URL}/estudos-middle/api/ccee/{tipo_cvu}/ultima-atualizacao',
        headers=get_auth_header()
    )
    if res.status_code == 200:
        return res.json()
    raise Exception(f"Erro ao obter data atualizacao: {res.status_code} - {res.text}")

def _get_cvu_data(tipo_cvu: str):
    res = requests.get(
        f'{constants.BASE_URL}/estudos-middle/api/ccee/{tipo_cvu}',
        headers=get_auth_header()
    )
    if res.status_code == 200:
        return res.json()
    raise Exception(f"Erro ao obter dados CVU: {res.status_code} - {res.text}")

def _post_cvu(body: dict, tipo_cvu: str):
    url = f'{constants.BASE_URL}/api/v2/decks/cvu'
    if tipo_cvu == 'merchant':
        url += '/merchant'
    res = requests.post(url, json=body, headers=get_auth_header())
    if res.status_code < 300:
        return res.json()
    raise Exception(f"Erro ao postar CVU: {res.status_code} - {res.text}")

def _check_cvu_status_processamento(tipo_cvu: str, data_atualizacao: str):
    res = requests.get(
        f"{constants.BASE_URL}/api/v2/decks/check-cvu",
        params={'tipo_cvu': tipo_cvu, 'data_atualizacao': data_atualizacao},
        headers=get_auth_header()
    )
    print(res.status_code, res.text)
    if res.status_code == 404:
        res = requests.post(
            f"{constants.BASE_URL}/api/v2/decks/check-cvu",
            json={'tipo_cvu': tipo_cvu, 'data_atualizacao': data_atualizacao, 'status': 'processando'},
            headers=get_auth_header()
        )
        return res.json()
    return res.json()

def _mark_cvu_as_processed(ids_cvu: list):
    for id_check_cvu in ids_cvu:
        res = requests.patch(
            f"{constants.BASE_URL}/api/v2/decks/check-cvu/{id_check_cvu}/status",
            params={'status': 'processado'},
            headers=get_auth_header()
        )
    return res.json()

@task.branch
def check_atualizacao():
    context = get_current_context()
    ti = context["ti"]
    dag_run = context["dag_run"]

    cvus_to_search = dag_run.conf.get('cvus_to_search', [])
    ids_to_modify = dag_run.conf.get('ids_to_modify', [])
    ultima_data_atualizacao = datetime.datetime.min
    ids_cvu_nao_processados = []

    tipos_cvu = _get_cvu_mapping()['tipos_cvu']
    if not cvus_to_search:
        for tipo_cvu in tipos_cvu:
            dt = _get_data_atualizacao(tipo_cvu)['data_atualizacao']
            status = _check_cvu_status_processamento(tipo_cvu, dt)
            if status.get('status') == 'processando':
                ids_cvu_nao_processados.append(status.get('id'))
                cvus_to_search.append(tipo_cvu)
                dt_parsed = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
                if dt_parsed > ultima_data_atualizacao:
                    ultima_data_atualizacao = dt_parsed

    tipos_cvu = [f"CCEE_{t}" for t in cvus_to_search]
    ti.xcom_push(key='ids_cvu_nao_processados', value=ids_cvu_nao_processados)
    ti.xcom_push(key='cvus_to_search', value=cvus_to_search)
    ti.xcom_push(key='dt_atualizacao', value=ultima_data_atualizacao.strftime('%Y-%m-%d'))

    dag_run.conf.update({
        'cvus_to_search': cvus_to_search,
        'tipos_cvu': tipos_cvu,
        'dt_atualizacao': ultima_data_atualizacao.strftime('%Y-%m-%d'),
        'task_to_execute': 'revisao_cvu',
        'ids_to_modify': ids_to_modify
    })

    if cvus_to_search:
        return "export_data_to_db"
    return "end_task"

@task
def export_data_to_db():
    context = get_current_context()
    ti = context["ti"]

    cvus_to_search = ti.xcom_pull(task_ids='check_atualizacao', key='cvus_to_search')
    ids_cvu = ti.xcom_pull(task_ids='check_atualizacao', key='ids_cvu_nao_processados')
    for tipo_cvu in cvus_to_search:
        _post_cvu(_get_cvu_data(tipo_cvu), tipo_cvu)
    _mark_cvu_as_processed(ids_cvu)

@task
def trigger_prospec_updater():
    context = get_current_context()
    ti = context["ti"]
    data_produto = datetime.datetime.strptime(ti.xcom_pull(task_ids='check_atualizacao', key='dt_atualizacao'), "%Y-%m-%d").strftime("%d/%m/%Y")
    cvus_to_search = ti.xcom_pull(task_ids='check_atualizacao', key='cvus_to_search')
    for cvu in cvus_to_search:
        trigger_dag("1.18-PROSPEC_UPDATE", conf={"tipo_cvu": cvu, "produto": "CVU", "dt_produto": data_produto})
    

@task
def end_task():
    pass
