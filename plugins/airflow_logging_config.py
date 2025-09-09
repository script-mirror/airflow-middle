import logging
from airflow.utils.log.file_task_handler import FileTaskHandler

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout',
        },
        'task': {
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'formatter': 'airflow.task',
            'base_log_folder': '/caminho/para/seu/diretorio/de/logs',  # Substitua pelo seu base_log_folder
        },
    },
    'loggers': {
        'airflow.task': {
            'handlers': ['task', 'console'],
            'level': 'INFO',
            'propagate': False,
        },
        'airflow.task.hooks.airflow.providers.ssh.hooks.ssh.SSHHook': {
            'handlers': ['task', 'console'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}