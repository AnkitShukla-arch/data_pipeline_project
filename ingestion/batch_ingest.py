import os
import pandas as pd
import yaml
from logging_audit.audit_logger import log_info, log_error
from ingestion.schema_validator import validate_schema


def load_config():
    with open("config/pipeline_config.yaml", "r") as f:
        return yaml.safe_load(f)


def batch_ingest(file_path: str, expected_schema: dict = None):
    """
    Batch ingestion for CSV, JSON, Parquet.
    Saves to staging as Parquet.
    """

    config = load_config()
    allowed_formats = config["pipeline"]["allowed_formats"]
    max_size = config["pipeline"]["max_file_size_mb"]

    if not os.path.exists(file_path):
        log_error(f"File not found: {file_path}")
        return None

    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if file_size_mb > max_size:
        log_error(f"File too large: {file_size_mb:.2f} MB")
        return None

    ext = file_path.split(".")[-1]
    if ext not in allowed_formats:
        log_error(f"Unsupported format: {ext}")
        return None

    log_info(f"Loading file: {file_path}")
    if ext == "csv":
        df = pd.read_csv(file_path)
    elif ext == "json":
        df = pd.read_json(file_path)
    elif ext == "parquet":
        df = pd.read_parquet(file_path)
    else:
        return None

    if expected_schema:
        if not validate_schema(df, expected_schema):
            return None

    os.makedirs("staging", exist_ok=True)
    out_path = f"staging/{os.path.basename(file_path).split('.')[0]}.parquet"
    df.to_parquet(out_path, index=False)
    log_info(f"File saved to staging: {out_path}")

    return out_path

