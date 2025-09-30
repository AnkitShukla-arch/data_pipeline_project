# 🚀 Data Pipeline Project  

An **end-to-end, industry-grade data pipeline** for **real-time + batch ingestion, transformation, ML model training, monitoring, and natural language querying**.  
Designed to be **production-ready** with CI/CD, lineage tracking, monitoring, and AI-powered query interface.  

---

## 📂 Project Structure  

data_pipeline_project/
│── README.md
│── requirements.txt
│── config/
│ ├── aws_config.yaml
│ ├── pipeline_config.yaml
│ ├── logging_config.yaml
│
│── dags/
│ ├── ingest_data_dag.py
│ ├── transform_dag.py
│ ├── model_training_dag.py
│ ├── monitoring_dag.py
│
│── ingestion/
│ ├── batch_ingest.py
│ ├── stream_ingest.py
│ ├── schema_validator.py
│
│── transformation/
│ ├── dbt_project/
│ │ ├── models/
│ │ │ ├── staging/
│ │ │ ├── marts/
│ │ │ ├── star_schema/
│ │ │ ├── snowflake_schema/
│ │ ├── dbt_project.yml
│ │ ├── profiles.yml
│ ├── transform_runner.py
│
│── warehouse/
│ ├── redshift_loader.py
│ ├── schema_manager.py
│
│── lineage/
│ ├── metadata_tracker.py
│ ├── lineage_logger.py
│
│── logging_audit/
│ ├── audit_logger.py
│ ├── error_handler.py
│
│── ml/
│ ├── training_pipeline.py
│ ├── model_registry.py
│ ├── drift_detector.py
│
│── monitoring/
│ ├── metrics_collector.py
│ ├── freshness_checker.py
│ ├── alert_manager.py
│
│── query_interface/
│ ├── llama_connector.py
│ ├── api_gateway.py
│
│── tests/
│ ├── test_ingestion.py
│ ├── test_transformations.py
│ ├── test_lineage.py
│ ├── test_ml_pipeline.py
│
│── scripts/
│ ├── bootstrap_pipeline.py
│ ├── deploy.sh
│
└── .github/
├── workflows/
├── ci_cd.yml


---

## ⚙️ Key Features  

- **Ingestion** → Batch (CSV/JSON/Parquet) + Streaming (Kafka/Kinesis)  
- **Transformation** → dbt models (staging, marts, star & snowflake schemas)  
- **Data Warehouse** → AWS Redshift with automated schema management  
- **Lineage & Metadata** → Glue Catalog / Atlas integration  
- **Logging & Audit** → Centralized logging + error handling with retries & alerts  
- **ML Pipeline** → Automated training, model registry, drift detection  
- **Monitoring** → Data freshness, pipeline metrics, Slack/Email alerts  
- **Query Interface** → LlamaIndex + API Gateway for **natural language querying**  
- **CI/CD** → GitHub Actions for testing, linting, and deployment  

---


## Installation
```bash
git clone https://github.com/your-username/data-pipeline.git
cd data-pipeline
python -m venv venv
source venv/bin/activate   # Linux/macOS
venv\Scripts\activate      # Windows
pip install -r requirements.txt


### 1️⃣ Install Dependencies  
```bash
pip install -r requirements.txt -c constraints.txt

2️⃣ Configure

Update configs in config/:
aws_config.yaml → AWS creds + Redshift configs
pipeline_config.yaml → ingestion & transformation rules
logging_config.yaml → logging/audit setup

3️⃣ Initialize Pipeline
python scripts/bootstrap_pipeline.py

4️⃣ Run with Airflow
airflow db init
airflow webserver --port 8080
airflow scheduler


👉 Access Airflow at: http://localhost:8080

🧠 Query with LlamaIndex

Once data is ingested and transformed:

from query_interface.llama_connector import LlamaWarehouseQuery

q = LlamaWarehouseQuery()
print(q.query("Show me the top 5 products by sales in last month"))


✅ Query your warehouse in plain English.
✅ Powered by LlamaIndex + SQL translation.

📈 Monitoring

Data freshness → monitoring/freshness_checker.py

Metrics collection → monitoring/metrics_collector.py

Alerts → Slack/Email via monitoring/alert_manager.py

🤖 CI/CD

Automated with GitHub Actions

Workflow file: .github/workflows/ci_cd.yml

Runs:

✅ Unit tests (tests/)

✅ Linting

✅ Deployment

🏆 Hackathon Highlight

This project is:

Fully automated → from ingestion → transformation → ML → query

Cloud-native → AWS Redshift + dbt + Airflow

AI-powered → Natural language queries via LlamaIndex

Enterprise-ready → Logging, lineage, monitoring, CI/CD

