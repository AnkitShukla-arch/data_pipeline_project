# dags/transform_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from transformation.transform_runner import run_dbt_transformations
from lineage.lineage_logger import log_lineage
from logging_audit.audit_logger import log_audit_event
from logging_audit.error_handler import handle_error


def transformation_task(**context):
    try:
        # Run dbt transformations
        run_dbt_transformations()

        # Log lineage
        log_lineage("dbt_transformations", context)

        # Audit log
        log_audit_event("Transformations executed successfully", context)
    except Exception as e:
        handle_error("Transformation step failed", e)
        raise


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email": ["alerts@datapipeline.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "transform_dag",
    default_args=default_args,
    description="Transformation DAG using dbt",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["transformation", "dbt", "pipeline"],
) as dag:

    transform = PythonOperator(
        task_id="run_transformations",
        python_callable=transformation_task,
        provide_context=True,
    )

    transform

