from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from llama_integration.query_interface import query_pipeline

def run_query():
    question = "Show me the model performance trends over time"
    answer = query_pipeline(question)
    print("Query:", question)
    print("Answer:", answer)

with DAG(
    dag_id="query_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["llama", "query"],
) as dag:

    query_task = PythonOperator(
        task_id="run_nl_query",
        python_callable=run_query
    )
