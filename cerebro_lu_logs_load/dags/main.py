from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from sness.settings.pod_type import PodType
from sness.pipeline.custom_operators.minecraft_operator import MinecraftOperator
from sness.pipeline.custom_operators.dataproc_operator import build_ness_etl_path
from sness.pipeline.slack import slack_failed_task

# Definição do Canal de Alerta (Certifique-se que esta variável está definida no seu ambiente)
CANAL_DE_ALERTA = "data-alerts-cerebro"

default_args = {
    'owner': 'squad-cerebro-lu',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime.strptime('2026-03-26', '%Y-%m-%d'),
    'on_failure_callback': slack_failed_task,
    #'slack_notification_channel': CANAL_DE_ALERTA,
    'params': {
        'labels': {
            'se_tribe': "ai-products",
            'se_vertical': "data-intelligence",
        }
    }
}

dag = DAG(
    "CerebroLuLogsLoad",
    catchup=False,
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1
)

start = DummyOperator(task_id='Start', dag=dag)
end = DummyOperator(task_id='End', dag=dag)

# Task 1: Ingestão da Tabela de Campanhas
ingest_campanhas = MinecraftOperator(
    task_id='ingest_campanhas',
    main=build_ness_etl_path('cerebro_lu_logs_load', 'raw', 'ingest_campanhas.py'),
    max_instances=2, # Ajustado para o volume de dados
    dag=dag
)

# Task 2: Ingestão da Tabela de Conversas
ingest_conversas = MinecraftOperator(
    task_id='ingest_conversas',
    main=build_ness_etl_path('cerebro_lu_logs_load', 'raw', 'ingest_conversas.py'),
    max_instances=4, # Conversas costuma ser uma tabela mais pesada
    dag=dag
)

# Fluxo de Execução: Inicia o processo e dispara as duas cargas simultaneamente
start >> [ingest_campanhas, ingest_conversas] >> end