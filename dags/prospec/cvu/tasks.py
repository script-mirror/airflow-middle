import requests
import datetime
import glob
import os
import re
from typing import List
from airflow.decorators import task
from middle.utils import Constants, get_auth_header, extract_zip
from middle.airflow import enviar_whatsapp_error as enviar_erro
from airflow.models.taskinstance import TaskInstance
from middle.sensibilidades.cvu import (
    get_cvu_trusted,
    atualizar_cvu_clast_estrutural,
    atualizar_cvu_clast_conjuntural,
    atualizar_cvu_dadger_decomp,
)
from middle.prospec import(
    get_ids_estudos,
    download_estudo,
    upload_estudo,
)
from middle.utils import setup_logger

logger = setup_logger()
constants = Constants()

@task
def enviar_whatsapp_erro(task_instance: TaskInstance = None):
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    enviar_erro(dag_id=dag_id, task_id=task_id, destinatario="airflow")
    enviar_erro(dag_id=dag_id, task_id=task_id, destinatario="debug")

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
def check_atualizacao(ti=None, dag_run=None):
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
def export_data_to_db(ti=None):
    cvus_to_search = ti.xcom_pull(task_ids='check_atualizacao', key='cvus_to_search')
    ids_cvu = ti.xcom_pull(task_ids='check_atualizacao', key='ids_cvu_nao_processados')
    for tipo_cvu in cvus_to_search:
        _post_cvu(_get_cvu_data(tipo_cvu), tipo_cvu)
    _mark_cvu_as_processed(ids_cvu)

@task
def end_task():
    pass


@task
def update_cvu_dadger_decomp(
    tipos_cvu: List[str],
    dt_atualizacao: datetime.datetime,
    ids_to_modify: List[int] = None
):
    tag = [f'CVU {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    if not ids_to_modify:
        ids_to_modify = get_ids_estudos()
    for tipo_cvu in tipos_cvu:
        info_cvu = get_cvu_trusted(
            tipo_cvu=tipo_cvu,
            dt_atualizacao=dt_atualizacao
        )

        for id_estudo in ids_to_modify:

            logger.info("\n\n")
            logger.info(f"Modificando estudo {id_estudo}")

            zip_content = download_estudo(id_estudo)['content']
            extracted_zip_estudo = extract_zip(
                zip_content,
                "/tmp"
            )
            
            if not os.path.exists(extracted_zip_estudo):
                logger.info(f"erro ao fazer download de estudo com id {id_estudo}")
                continue

            dadgers_to_modify = glob.glob(
                os.path.join(
                    extracted_zip_estudo,
                    "**",
                    "*dadger*"),
                recursive=True)
            arquivos_filtrados = [
                arquivo for arquivo in dadgers_to_modify if not re.search(
                    r'\.0+$', arquivo)]
            if arquivos_filtrados == []:
                raise Exception(
                    "Não foi encontrado nenhum arquivo"
                    f" dadger no estudo {id_estudo}")
            paths_modified = atualizar_cvu_dadger_decomp(
                info_cvu,
                arquivos_filtrados,
                id_estudo
            )

            upload_estudo(id_estudo, paths_modified, tag)
            os.remove(extracted_zip_estudo)


@task
def update_cvu_clast_newave(tipos_cvu=None, dt_atualizacao=None, ids_to_modify=None):
    if isinstance(tipos_cvu, str):
        tipos_cvu = eval(tipos_cvu)
        
    if isinstance(ids_to_modify, str):
        ids_to_modify = eval(ids_to_modify) if ids_to_modify else None
        
    dt_atualizacao = datetime.datetime.strptime(dt_atualizacao, '%Y-%m-%d') if dt_atualizacao else datetime.datetime.now()
    
    for tipo_cvu in tipos_cvu:
        tipo_cvu = tipo_cvu.replace('CCEE_', '')
        
        logger.info(f"Updating CVU in CLAST files for NEWAVE studies: {tipo_cvu}")
        logger.info(f"Date of update: {dt_atualizacao}")
        logger.info(f"IDs to modify: {ids_to_modify}")
        
        paths_modified = []
        tag = [f'CVU {datetime.datetime.now().strftime("%d/%m %H:%M")}']
        if not ids_to_modify:
            ids_to_modify = get_ids_estudos()

        df_cvu = get_cvu_trusted(
            tipo_cvu=tipo_cvu,
            dt_atualizacao=dt_atualizacao
        )
        
        for id_estudo in ids_to_modify:
            logger.info(f"Modificando estudo {id_estudo}")

            zip_content = download_estudo(id_estudo)['content']
            extracted_zip_estudo = extract_zip(
                zip_content,
                "/tmp"
            )
            
            estudo_contem_newave = [
                nome for nome in os.listdir(extracted_zip_estudo) 
                if os.path.isdir(os.path.join(extracted_zip_estudo, nome)) 
                and nome.startswith("NW")
            ]
            if not estudo_contem_newave:
                logger.info(f"Estudo {id_estudo} nao possui Newave")
                continue

            clast_to_modify = glob.glob(
                os.path.join(
                    extracted_zip_estudo,
                    "**",
                    "*clast*"
                ),
                recursive=True)
            arquivos_filtrados = [
                arquivo for arquivo in clast_to_modify if not re.search(
                    r'\.0+$', arquivo)]
            if arquivos_filtrados == []:
                logger.warning(
                    f"Não foi encontrado nenhum arquivo clast no estudo {id_estudo}")
                continue
                
            if tipo_cvu.split("_")[0] == 'conjuntural':
                paths_modified += atualizar_cvu_clast_conjuntural(
                    arquivos_filtrados,
                    df_cvu,
                )
            elif tipo_cvu == 'estrutural' or tipo_cvu == 'merchant':
                paths_modified += atualizar_cvu_clast_estrutural(
                    arquivos_filtrados,
                    df_cvu,
                )

            if paths_modified:
                upload_estudo(id_estudo, paths_modified, tag)
                os.remove(extracted_zip_estudo)

    return True
