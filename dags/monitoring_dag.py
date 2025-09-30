# dags/monitoring_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from monitoring.metrics_collector import collect_pipeline_metrics
from monitoring.freshness_checker import check_data_freshness
from monitoring.alert_manager import send_alert
from logging_audit.audit_logger import log_audit_event
from logging_audit.error_handler import handle_error
from lineage.lineage_logger import log_lineage


def monitoring_task(**context):
    try:
        # 1. Collect pipeline metrics
        metrics = collect_pipeline_metrics()

        # 2. Check data freshness
        is_fresh = check_data_freshness()
        if not is_fresh:
            send_alert("Data freshness check FAILED 🚨")
            log_audit_event("Data freshness issue detected", context)

        # 3. Log lineage of monitoring
        log_lineage("monitoring", {"metrics": metrics, "data_fresh": is_fresh})

        # 4. Audit logging
        log_audit_event(f"Monitoring completed. Metrics: {metrics}", context)

    except Exception as e:
        handle_error("Monitoring task failed", e)
        raise


default_args = {
    "owner": "data_platform",
    "depends_on_past": False,
    "email": ["alerts@datapipeline.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "monitoring_dag",
    default_args=default_args,
    description="Pipeline Monitoring & Alerts DAG",
    schedule_interval="0 * * * *",  # runs hourly
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["monitoring", "alerts", "pipeline"],
) as dag:

    monitoring = PythonOperator(
        task_id="run_monitoring",
        python_callable=monitoring_task,
        provide_context=True,
    )

    monitoring
