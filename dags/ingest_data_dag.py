# dags/ingest_data_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.batch_ingest import run_batch_ingestion
from ingestion.stream_ingest import run_stream_ingestion
from ingestion.schema_validator import validate_schema
from logging_audit.audit_logger import log_audit_event
from logging_audit.error_handler import handle_error


def batch_ingest_task(**context):
    try:
        validate_schema()
        run_batch_ingestion()
        log_audit_event("Batch ingestion successful", context)
    except Exception as e:
        handle_error("Batch ingestion failed", e)
        raise


def stream_ingest_task(**context):
    try:
        run_stream_ingestion()
        log_audit_event("Stream ingestion successful", context)
    except Exception as e:
        handle_error("Stream ingestion failed", e)
        raise


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email": ["alerts@datapipeline.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ingest_data_dag",
    default_args=default_args,
    description="Ingestion DAG for batch & stream pipelines",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ingestion", "pipeline"],
) as dag:

    batch_ingest = PythonOperator(
        task_id="batch_ingest",
        python_callable=batch_ingest_task,
        provide_context=True,
    )

    stream_ingest = PythonOperator(
        task_id="stream_ingest",
        python_callable=stream_ingest_task,
        provide_context=True,
    )

    batch_ingest >> stream_ingest

