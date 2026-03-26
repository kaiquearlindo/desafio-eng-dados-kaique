"""
campanhas_to_bigquery.py
------------------------
DAG Airflow para ingestão dos arquivos campanhas.json e conversas.json
para tabelas no BigQuery.

Projeto  : prd-data-products-ai
Dataset  : cerebro_da_lu
Tabelas  : campanhas, conversas
Schedule : @daily (reprocessamento incremental via publish_time)
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# ============================================================
# CONFIGURAÇÕES
# ============================================================

PROJECT_ID   = "prd-data-products-ai"
DATASET_ID   = "cerebro_da_lu"
GCS_BUCKET   = "cerebro-da-lu-landing"
GCS_PREFIX   = "raw/campanhas"

CAMPANHAS_FILE = "/opt/airflow/data/campanhas.json"
CONVERSAS_FILE = "/opt/airflow/data/conversas.json"

# Schemas BigQuery

SCHEMA_CAMPANHAS = [
    {"name": "session_id",         "type": "STRING",    "mode": "NULLABLE"},
    {"name": "source",             "type": "STRING",    "mode": "NULLABLE"},
    {"name": "ctwa_clid",          "type": "STRING",    "mode": "NULLABLE"},
    {"name": "channel_client_id",  "type": "STRING",    "mode": "NULLABLE"},
    {"name": "publish_time",       "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "data",               "type": "STRING",    "mode": "NULLABLE"},
    {"name": "attributes",         "type": "STRING",    "mode": "NULLABLE"},
    {"name": "subscription_name",  "type": "STRING",    "mode": "NULLABLE"},
    {"name": "message_id",         "type": "STRING",    "mode": "NULLABLE"},
    {"name": "template",           "type": "STRING",    "mode": "NULLABLE"},
    {"name": "version",            "type": "STRING",    "mode": "NULLABLE"},
    # Campo de auditoria adicionado na ingestão
    {"name": "ingested_at",        "type": "TIMESTAMP", "mode": "NULLABLE"},
]

SCHEMA_CONVERSAS = [
    {"name": "session_id",         "type": "STRING",    "mode": "NULLABLE"},
    {"name": "text",               "type": "STRING",    "mode": "NULLABLE"},
    {"name": "author",             "type": "STRING",    "mode": "NULLABLE"},
    {"name": "user_id",            "type": "STRING",    "mode": "NULLABLE"},
    {"name": "publish_time",       "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "data",               "type": "STRING",    "mode": "NULLABLE"},
    {"name": "attributes",         "type": "STRING",    "mode": "NULLABLE"},
    {"name": "subscription_name",  "type": "STRING",    "mode": "NULLABLE"},
    {"name": "message_id",         "type": "STRING",    "mode": "NULLABLE"},
    {"name": "media_type",         "type": "STRING",    "mode": "NULLABLE"},
    {"name": "ingested_at",        "type": "TIMESTAMP", "mode": "NULLABLE"},
]

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def _validate_and_enrich_json(input_path: str, output_path: str, table: str) -> None:
    """
    Lê o JSON, adiciona campo de auditoria ingested_at,
    converte para JSONL (um objeto por linha) para o BigQuery.
    Também loga warnings para registros com campos críticos nulos.
    """
    with open(input_path) as f:
        records = json.load(f)

    ingested_at = datetime.utcnow().isoformat() + "Z"
    warnings = []

    with open(output_path, "w") as out:
        for i, record in enumerate(records):
            record["ingested_at"] = ingested_at

            # Validação: campos críticos
            if table == "campanhas":
                if not record.get("version") or record["version"] == "1":
                    warnings.append({
                        "row"      : i,
                        "session_id": record.get("session_id"),
                        "template" : record.get("template"),
                        "version"  : record.get("version"),
                        "issue"    : "version inválida ou genérica",
                    })
                if not record.get("template"):
                    warnings.append({
                        "row"      : i,
                        "session_id": record.get("session_id"),
                        "issue"    : "template ausente",
                    })

            out.write(json.dumps(record, ensure_ascii=False) + "\n")

    if warnings:
        print(f"⚠️  {len(warnings)} warnings de qualidade encontrados em {table}:")
        for w in warnings[:10]:  # mostra até 10 no log
            print(f"   {w}")
        if len(warnings) > 10:
            print(f"   ... e mais {len(warnings) - 10} warnings")

    print(f"✅ {len(records)} registros processados para {table}")


# ============================================================
# DAG
# ============================================================

default_args = {
    "owner"           : "data-engineering",
    "depends_on_past" : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": True,
    "email"           : ["data-team@magalu.com"],
}

with DAG(
    dag_id          = "campanhas_json_to_bigquery",
    default_args    = default_args,
    description     = "Ingestão de campanhas.json e conversas.json para o BigQuery (cerebro_da_lu)",
    schedule_interval = "@daily",
    start_date      = days_ago(1),
    catchup         = False,
    tags            = ["cerebro-da-lu", "crm", "campanhas", "ingestão"],
) as dag:

    # ----------------------------------------------------------
    # CAMPANHAS
    # ----------------------------------------------------------

    validate_campanhas = PythonOperator(
        task_id         = "validate_campanhas",
        python_callable = _validate_and_enrich_json,
        op_kwargs       = {
            "input_path" : CAMPANHAS_FILE,
            "output_path": "/tmp/campanhas.jsonl",
            "table"      : "campanhas",
        },
    )

    upload_campanhas_gcs = LocalFilesystemToGCSOperator(
        task_id      = "upload_campanhas_gcs",
        src          = "/tmp/campanhas.jsonl",
        dst          = f"{GCS_PREFIX}/campanhas.jsonl",
        bucket       = GCS_BUCKET,
        gcp_conn_id  = "google_cloud_default",
    )

    load_campanhas_bq = GCSToBigQueryOperator(
        task_id             = "load_campanhas_bq",
        bucket              = GCS_BUCKET,
        source_objects      = [f"{GCS_PREFIX}/campanhas.jsonl"],
        destination_project_dataset_table = f"{PROJECT_ID}.{DATASET_ID}.campanhas",
        source_format       = "NEWLINE_DELIMITED_JSON",
        schema_fields       = SCHEMA_CAMPANHAS,
        write_disposition   = "WRITE_APPEND",        # incremental
        time_partitioning   = {
            "type" : "DAY",
            "field": "publish_time",
        },
        cluster_fields      = ["template", "version"],
        create_disposition  = "CREATE_IF_NEEDED",
        gcp_conn_id         = "google_cloud_default",
    )

    # ----------------------------------------------------------
    # CONVERSAS
    # ----------------------------------------------------------

    validate_conversas = PythonOperator(
        task_id         = "validate_conversas",
        python_callable = _validate_and_enrich_json,
        op_kwargs       = {
            "input_path" : CONVERSAS_FILE,
            "output_path": "/tmp/conversas.jsonl",
            "table"      : "conversas",
        },
    )

    upload_conversas_gcs = LocalFilesystemToGCSOperator(
        task_id      = "upload_conversas_gcs",
        src          = "/tmp/conversas.jsonl",
        dst          = f"{GCS_PREFIX}/conversas.jsonl",
        bucket       = GCS_BUCKET,
        gcp_conn_id  = "google_cloud_default",
    )

    load_conversas_bq = GCSToBigQueryOperator(
        task_id             = "load_conversas_bq",
        bucket              = GCS_BUCKET,
        source_objects      = [f"{GCS_PREFIX}/conversas.jsonl"],
        destination_project_dataset_table = f"{PROJECT_ID}.{DATASET_ID}.conversas",
        source_format       = "NEWLINE_DELIMITED_JSON",
        schema_fields       = SCHEMA_CONVERSAS,
        write_disposition   = "WRITE_APPEND",
        time_partitioning   = {
            "type" : "DAY",
            "field": "publish_time",
        },
        cluster_fields      = ["session_id", "author"],
        create_disposition  = "CREATE_IF_NEEDED",
        gcp_conn_id         = "google_cloud_default",
    )

    # ----------------------------------------------------------
    # DEPENDÊNCIAS
    # ----------------------------------------------------------

    validate_campanhas >> upload_campanhas_gcs >> load_campanhas_bq
    validate_conversas >> upload_conversas_gcs >> load_conversas_bq
