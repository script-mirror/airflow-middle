from middle.airflow import enviar_whatsapp_erro as enviar_erro
from typing import Union, List, Optional

def enviar_whatsapp_erro(
    task_instance=None,
    destinatarios: Union[str, List[str]] = None,
    dag_id_custom: Optional[str] = None,
    task_id_custom: Optional[str] = None
):
    """
    Envia uma mensagem de erro via WhatsApp quando uma tarefa falha.
    
    Args:
        task_instance: Instância da task do Airflow
        destinatarios: Destinatário(s) - pode ser string única ou lista de strings
        dag_id_custom: DAG ID customizado (opcional)
        task_id_custom: Task ID customizado (opcional)
    """
    dag_id = dag_id_custom or (task_instance.dag_id if task_instance else "unknown_dag")
    task_id = task_id_custom or (task_instance.task_id if task_instance else "unknown_task")
    
    if isinstance(destinatarios, str):
        destinatarios = [destinatarios]
        
    for destinatario in destinatarios:
        enviar_erro(dag_id=dag_id, task_id=task_id, destinatario=destinatario)
    

def callback_whatsapp_erro_padrao(context):
    """
    Callback padrão que pode ser usado diretamente no on_failure_callback.
    Extrai task_instance do contexto e chama enviar_whatsapp_erro.
    """
    task_instance = context.get('task_instance')
    return enviar_whatsapp_erro(task_instance=task_instance)