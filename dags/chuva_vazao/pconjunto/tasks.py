import datetime
from airflow.decorators import task
from middle.utils import (
    Constants,
    setup_logger,
)
from middle.s3 import get_latest_webhook_product

logger = setup_logger()
constants = Constants()


@task
def start_task(**kwargs):
    return None

@task
def end_task(**kwargs):
    return None

@task
def check_ecmwf(data_execucao: str, **kwargs):
    items = get_latest_webhook_product(
        constants.WEBHOOK_ECMWF,
        datetime.datetime.strptime(data_execucao, '%Y-%m-%d'),
        date_range=0,
    )
    if not items:
        raise ValueError(f'ECMWF dia {data_execucao} nao encontrado')
    else:
        logger.info(f'GEFS dia {data_execucao} ok')
    return None

@task
def check_gefs(data_execucao: str, **kwargs):
    items = get_latest_webhook_product(
        constants.WEBHOOK_GEFS,
        datetime.datetime.strptime(data_execucao, '%Y-%m-%d'),
        date_range=0,
    )
    if not items:
        raise ValueError(f'GEFS dia {data_execucao} nao encontrado')
    else:
        logger.info(f'GEFS dia {data_execucao} ok')
    return None

@task
def check_eta(data_execucao: str, **kwargs):
    items = get_latest_webhook_product(
        constants.WEBHOOK_ETA,
        datetime.datetime.strptime(data_execucao, '%Y-%m-%d'),
        date_range=0,
    )
    if not items:
        raise ValueError(f'ETA dia {data_execucao} nao encontrado')
    else:
        logger.info(f'GEFS dia {data_execucao} ok')
    return None