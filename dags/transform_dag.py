# dags/transform_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import transformation runner
from transformation.transform_runner import run_dbt_transformations

# Import monitoring modules
from monitoring.metrics_collector import collect_metrics
from monitoring.freshness_checker import check_freshness
from monitoring.alert_manager import send_alert

# Import lineage + audit
from lineage.lineage_logger import log_lineage
from logging_audit.audit_logger import log_audit

default_args = {
    "owner": "data_eng_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@company.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="transform_dag",
    default_args=default_args,
    description="Transformation DAG with monitoring, lineage, and audit logging",
    schedule_interval="@daily",  # run once daily, adjust as needed
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["transformation", "monitoring", "lineage"],
) as dag:

    # Step 1: Run dbt transformations
    run_transformations = PythonOperator(
        task_id="run_transformations",
        python_callable=run_dbt_transformations,
    )

    # Step 2: Collect pipeline metrics
    collect_metrics_task = PythonOperator(
        task_id="collect_metrics",
        python_callable=collect_metrics,
    )

    # Step 3: Check data freshness
    check_freshness_task = PythonOperator(
        task_id="check_freshness",
        python_callable=check_freshness,
    )

    # Step 4: Log lineage
    log_lineage_task = PythonOperator(
        task_id="log_lineage",
        python_callable=log_lineage,
    )

    # Step 5: Audit log
    log_audit_task = PythonOperator(
        task_id="log_audit",
        python_callable=log_audit,
    )

    # Step 6: Send alert if needed
    send_alert_task = PythonOperator(
        task_id="send_alert",
        python_callable=send_alert,
    )

    # Dependencies
    run_transformations >> collect_metrics_task >> check_freshness_task
    check_freshness_task >> log_lineage_task >> log_audit_task >> send_alert_task
