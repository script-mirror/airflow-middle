from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from utils.sender_message import callback_whatsapp_erro_padrao
from middle.constants import WEBHOOK_TASKS_PATH
from datetime import datetime, timedelta


WEBHOOK_PRODUCTS = {
    "Relatório de Acompanhamento Hidrológico": {
        "description": "DAG do Relatório de Acompanhamento Hidrológico",
        "tags": ["relatorio", "acompanhamento", "hidrologico"],
        "file": "relatorio_acompanhamento_hidrologico.py",
    },
    "Precipitação por Satélite.": {
        "description": "DAG da Precipitação por Satélite",
        "tags": ["precipitacao", "satelite"],
        "file": "precipitacao_satelite.py",
    },
    "Modelo GEFS": {
        "description": "DAG do Modelo GEFS",
        "tags": ["modelo", "gefs"],
        "file": "modelo_gefs.py",
    },
    "Resultados preliminares não consistidos  (vazões semanais - PMO)": {
        "description": "DAG dos Resultados Preliminares Não Consistidos de Vazões Semanais PMO",
        "tags": ["resultados", "preliminares", "nao_consistidos", "vazoes", "semanais", "pmo"],
        "file": "resultados_preliminares_nao_consistidos_vazoes_semanais_pmo.py",
    },
    "Relatório dos resultados finais consistidos da previsão diária (PDP)": {
        "description": "DAG do Relatório de Resultados Finais Consistidos de Previsão Diária PDP",
        "tags": ["relatorio", "resultados", "finais", "consistidos", "previsao", "diaria", "pdp"],
        "file": "relatorio_resultados_finais_consistidos_previsao_diaria_pdp.py",
    },
    "Níveis de Partida para o DESSEM": {
        "description": "DAG dos Níveis de Partida DESSEM",
        "tags": ["niveis", "partida", "dessem"],
        "file": "niveis_partida_dessem.py",
    },
    "DADVAZ – Arquivo de Previsão de Vazões Diárias (PDP)": {
        "description": "DAG do DADVAZ Previsão de Vazões Diárias PDP",
        "tags": ["dadvaz", "vazoes", "pdp", "diarias"],
        "file": "dadvaz_previsao_vazoes_diarias_pdp.py",
    },
    "Deck e Resultados DECOMP - Valor Esperado": {
        "description": "DAG do Deck Resultados DECOMP Valor Esperado",
        "tags": ["deck", "decomp", "valor_esperado", "resultados"],
        "file": "deck_resultados_decomp_valor_esperado.py",
    },
    "Resultados finais consistidos (vazões diárias - PDP)": {
        "description": "DAG dos Resultados Finais Consistidos de Vazões Diárias PDP",
        "tags": ["resultados", "finais", "consistidos", "vazoes", "diarias", "pdp"],
        "file": "resultados_finais_consistidos_vazoes_diarias_pdp.py",
    },
    "Resultados preliminares consistidos (vazões semanais - PMO)": {
        "description": "DAG dos Resultados Preliminares Consistidos de Vazões Semanais PMO",
        "tags": ["resultados", "preliminares", "consistidos", "vazoes", "semanais", "pmo"],
        "file": "resultados_preliminares_consistidos_vazoes_semanais_pmo.py",
    },
    "Arquivos dos modelos de previsão de vazões semanais - PMO": {
        "description": "DAG dos Arquivos dos Modelos de Previsão de Vazões Semanais PMO",
        "tags": ["vazoes", "pmo", "semanais"],
        "file": "arquivos_modelos_previsao_vazoes_semanais_pmo.py",
    },
    "Arquivos dos modelos de previsão de vazões diárias - PDP": {
        "description": "DAG dos Arquivos dos Modelos de Previsão de Vazões Diárias PDP",
        "tags": ["vazoes", "pdp", "diarias"],
        "file": "arquivos_modelos_previsao_vazoes_diarias_pdp.py",
    },
    "Acomph": {
        "description": "DAG do produto ACOMPH",
        "tags": ["acomph"],
        "file": "acomph.py",
    },
    "RDH": {
        "description": "DAG do produto RDH",
        "tags": ["rdh"],
        "file": "rdh.py",
    },
    "Histórico de Precipitação por Satélite": {
        "description": "DAG do Histórico de Precipitação por Satélite",
        "tags": ["historico", "precipitacao", "satelite"],
        "file": "historico_precipitacao_satelite.py",
    },
    "Modelo ETA": {
        "description": "DAG do Modelo ETA",
        "tags": ["modelo", "eta"],
        "file": "modelo_eta.py",
    },
    "Carga por patamar - DECOMP": {
        "description": "DAG da Carga Patamar DECOMP",
        "tags": ["carga", "patamar", "decomp"],
        "file": "carga_patamar_decomp.py",
    },
    "Deck Preliminar DECOMP - Valor Esperado": {
        "description": "DAG do Deck Preliminar DECOMP Valor Esperado",
        "tags": ["deck", "decomp", "valor_esperado", "preliminar"],
        "file": "deck_preliminar_decomp_valor_esperado.py",
    },
    "Decks de entrada e saída - Modelo DESSEM": {
        "description": "DAG dos Decks de Entrada e Saída do Modelo DESSEM",
        "tags": ["decks", "entrada", "saida", "modelo", "dessem"],
        "file": "decks_entrada_saida_modelo_dessem.py",
    },
    "Arquivos de Previsão de Carga para o DESSEM": {
        "description": "DAG dos Arquivos de Previsão de Carga DESSEM",
        "tags": ["carga", "dessem"],
        "file": "arquivos_previsao_carga_dessem.py",
    },
    "Decks de entrada do PrevCargaDESSEM": {
        "description": "DAG dos Decks de Entrada PREVCARGA DESSEM",
        "tags": ["decks", "entrada", "prevcarga", "dessem"],
        "file": "decks_entrada_prevcarga_dessem.py",
    },
    "Previsões de carga mensal e por patamar - NEWAVE": {
        "description": "DAG das Previsões de Carga Mensal por Patamar NEWAVE",
        "tags": ["previsoes", "carga", "mensal", "patamar", "newave"],
        "file": "previsoes_carga_mensal_patamar_newave.py",
    },
    "IPDO (Informativo Preliminar Diário da Operação)": {
        "description": "DAG do produto IPDO (Informativo Preliminar Diário da Operação)",
        "tags": ["ipdo"],
        "file": "ipdo_informativo_preliminar_diario.py",
    },
    "Modelo ECMWF": {
        "description": "DAG do Modelo ECMWF",
        "tags": ["modelo", "ecmwf"],
        "file": "modelo_ecmwf.py",
    },
    "Dados utilizados na previsão de geração eólica": {
        "description": "DAG dos Dados de Previsão de Geração Eólica",
        "tags": ["eolica", "geracao"],
        "file": "dados_previsao_geracao_eolica.py",
    },
    "Arquivos de Previsão de Carga para o DESSEM - PrevCargaDESSEM": {
        "description": "DAG dos Arquivos de Previsão de Carga DESSEM PREVCARGA",
        "tags": ["carga", "dessem", "prevcarga"],
        "file": "arquivos_previsao_carga_dessem_prevcarga.py",
    },
    "Deck NEWAVE Preliminar": {
        "description": "DAG do Deck NEWAVE Preliminar",
        "tags": ["deck", "newave", "preliminar"],
        "file": "deck_newave_preliminar.py",
    },
    "DECK NEWAVE DEFINITIVO": {
        "description": "DAG dos Decks NEWAVE",
        "tags": ["decks", "newave"],
        "file": "decks_newave.py",
    },
    "DECKS DA PREVISÃO DE GERAÇÃO EÓLICA SEMANAL WEOL-SM": {
        "description": "DAG dos Decks de Previsão de Geração Eólica Semanal WEOLSM",
        "tags": ["decks", "eolica", "semanal", "weolsm"],
        "file": "decks_previsao_geracao_eolica_semanal_weolsm.py",
    },
    "Preliminar - Relatório Mensal de Limites de Intercâmbio": {
        "description": "DAG do Preliminar Relatório de Limites de Intercâmbio",
        "tags": ["preliminar", "relatorio", "limites", "intercambio"],
        "file": "preliminar_relatorio_limites_intercambio.py",
    },
    "Relatório Mensal de Limites de Intercâmbio para o Modelo DECOMP": {
        "description": "DAG do Relatório de Limites de Intercâmbio do Modelo DECOMP",
        "tags": ["relatorio", "limites", "intercambio", "modelo", "decomp"],
        "file": "relatorio_limites_intercambio_modelo_decomp.py",
    },
    "Notas Técnicas - Medio Prazo": {
        "description": "DAG das Notas Técnicas de Médio Prazo",
        "tags": ["notas", "tecnicas", "medio_prazo"],
        "file": "notas_tecnicas_medio_prazo.py",
    }
}

def get_product_details(**kwargs):
    """
    Obtém os detalhes do produto a partir do payload do produto triggado.
    Returns:
        Detalhes do produto ou None se não encontrado.
    """
    product_details = kwargs.get('dag_run').conf
    return product_details

def remover_acentos_e_caracteres_especiais(texto):
    import re
    import unicodedata
    texto_norm = unicodedata.normalize('NFD', texto)
    texto_limpo = re.sub(r'[^\w\s]', '', texto_norm)
    texto_limpo = re.sub(r'\W+', '_', texto_limpo)
    return texto_limpo

@dag(
    dag_id="WEBHOOK_PRODUCTS",
    description="DAG consolidada para processamento de todos os produtos webhook",
    catchup=False,
    default_args={
        "owner": "trading-middle",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": callback_whatsapp_erro_padrao
    },
    tags=["webhook", "products", "consolidated"],
    render_template_as_native_obj=True,
)
def dag_webhook():
    """
    DAG consolidada para processamento de todos os produtos webhook.
    
    Esta DAG itera sobre o objeto WEBHOOK_PRODUCTS e cria tasks dinâmicas
    para cada produto configurado. Quando um produto tem um path definido,
    a task SSH executará o comando no path especificado do libbles middle.
    """
    
    inicio = EmptyOperator(task_id="inicio")
    final = EmptyOperator(task_id="final")
    
    for product_key, product_config in WEBHOOK_PRODUCTS.items():
        
        if product_config.get('path') and product_config.get('file'):
            command = f"""cd {WEBHOOK_TASKS_PATH} && python {product_config['file']} \
                          --conf='{{{{ dag_run.conf | tojson }}}}'"""
        else:
            command = f"echo 'Produto {product_key} aguardando configuração de path e arquivo'"
            
        clean_name = remover_acentos_e_caracteres_especiais(product_key)
        upper_case_name = clean_name.upper()
    
        run_product_task = SSHOperator(
            task_id=upper_case_name,
            command=command,
            ssh_conn_id="ssh_default"
        )
        
        inicio >> run_product_task >> final
    

dag_webhook = dag_webhook()
