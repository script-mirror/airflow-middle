import logging
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = DEFAULT_LOGGING_CONFIG.copy()

# Remove o campo "source" dos formatadores padrão
LOGGING_CONFIG["formatters"]["airflow"] = {
    "format": "%(asctime)s - %(levelname)s - %(message)s",
    "class": "logging.Formatter",
}

LOGGING_CONFIG["formatters"]["task"] = {
    "format": "%(asctime)s - %(levelname)s - %(message)s",
    "class": "logging.Formatter",
}
