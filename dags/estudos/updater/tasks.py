import os
from airflow.decorators import task

PATH_PROJETOS: str = os.getenv('PATH_PROJETOS')
ATIVAR_ENV: str = os.getenv('ATIVAR_ENV')
CMD_BASE = str(ATIVAR_ENV) + " python " + str(PATH_PROJETOS) + "/estudos-middle/estudos_prospec/main_roda_estudos.py "

@task
def prepare_update_command(**kwargs):
    """
    Prepara o comando para atualização do PROSPEC com base nos parâmetros recebidos
    """
    params = kwargs.get('params', {})
    
    command = f"{CMD_BASE} prevs UPDATE rodada Preliminar"
    
    for key, value in params.items():
        if value is not None:
            command += f" {key} '{value}'"
    
    return command

