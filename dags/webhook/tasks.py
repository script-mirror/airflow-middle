from airflow.decorators import task
import requests
from middle.utils import (
    get_auth_header,
    Constants,
    setup_logger,
    sanitize_string,
)

logger = setup_logger()
constants = Constants()

@task.branch
def start_task(**kwargs):
    params = kwargs.get('dag_run').conf
    product_name = params.get('nome')
    task_id = sanitize_string(product_name, space_char='_',)
    return [task_id]


@task
def end_task(**kwargs):
    return None

def executar_task(**kwargs):
    params = kwargs.get('dag_run').conf
    product_name = params.get('nome')
    res = requests.post(
        f"{constants.BASE_URL}/tasks/api/webhook",
        headers=get_auth_header(),
        json=params,
    )
    try:
        res.raise_for_status()
        logger.info(f"API chamada com sucesso para o produto {product_name}: {res.text}")
    except Exception:
        logger.error(f"Erro ao chamar API para o produto {product_name}: {res.text}")
        raise Exception(f"Erro ao chamar API para o produto {product_name}: {res.text}")
    return ["end_task"]
