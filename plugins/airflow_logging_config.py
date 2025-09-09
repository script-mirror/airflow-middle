import logging
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG['formatters']['airflow.task'] = {
    'format': '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
}

LOGGING_CONFIG['handlers']['console'] = {
    'class': 'logging.StreamHandler',
    'formatter': 'airflow.task',
    'stream': 'ext://sys.stdout',
}

LOGGING_CONFIG['handlers']['task'] = {
    'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
    'formatter': 'airflow.task',
    'base_log_folder': '/opt/airflow/logs/tasks',
    'filename_template': '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log',
}

LOGGING_CONFIG['loggers']['airflow.task'] = {
    'handlers': ['task', 'console'],
    'level': 'INFO',
    'propagate': False,
}