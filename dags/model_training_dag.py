# dags/model_training_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ml.training_pipeline import train_and_register_model
from ml.drift_detector import check_data_drift
from ml.model_registry import register_model_version
from logging_audit.audit_logger import log_audit_event
from logging_audit.error_handler import handle_error
from lineage.lineage_logger import log_lineage


def training_task(**context):
    try:
        # 1. Check for data drift before training
        drift_detected = check_data_drift()
        if drift_detected:
            log_audit_event("Data drift detected, retraining triggered", context)

        # 2. Train + Register Model
        model_info = train_and_register_model()

        # 3. Store model version in registry
        register_model_version(model_info)

        # 4. Log lineage
        log_lineage("model_training", {"model_info": model_info})

        # 5. Audit logging
        log_audit_event(f"Model trained and registered: {model_info}", context)

    except Exception as e:
        handle_error("Model training pipeline failed", e)
        raise


default_args = {
    "owner": "ml_team",
    "depends_on_past": False,
    "email": ["alerts@datapipeline.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    "model_training_dag",
    default_args=default_args,
    description="ML Model Training + Drift Detection DAG",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ml", "training", "pipeline"],
) as dag:

    model_training = PythonOperator(
        task_id="train_model",
        python_callable=training_task,
        provide_context=True,
    )

    model_training

