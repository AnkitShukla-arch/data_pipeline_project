import os
import sys
import logging
import boto3
import pandas as pd
import yaml
from botocore.exceptions import ClientError
from ingestion.schema_validator import SchemaValidator

# Load logging config
import logging.config
import pathlib

LOG_CONFIG = pathlib.Path(__file__).parents[1] / "config" / "logging_config.yaml"
if LOG_CONFIG.exists():
    with open(LOG_CONFIG, "r") as f:
        logging.config.dictConfig(yaml.safe_load(f))
logger = logging.getLogger("batch_ingest")


class BatchIngest:
    def __init__(self, aws_config_path: str, pipeline_config_path: str):
        self.aws_config = self._load_yaml(aws_config_path)["aws"]
        self.pipeline_config = self._load_yaml(pipeline_config_path)["ingestion"]["batch"]

        # AWS S3 client
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_config["access_key_id"],
            aws_secret_access_key=self.aws_config["secret_access_key"],
            region_name=self.aws_config["region"],
        )

        self.bucket = self.aws_config["s3_bucket"]
        self.s3_target = self.pipeline_config["s3_target"]

    @staticmethod
    def _load_yaml(path: str):
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _read_file(self, file_path: str) -> pd.DataFrame:
        ext = os.path.splitext(file_path)[-1].lower()
        if ext == ".csv":
            return pd.read_csv(file_path)
        elif ext == ".json":
            return pd.read_json(file_path, lines=True)
        elif ext == ".parquet":
            return pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    def _upload_to_s3(self, df: pd.DataFrame, file_name: str):
        try:
            target_key = os.path.join(self.s3_target, file_name)
            buffer = df.to_parquet(index=False)
            self.s3.put_object(Bucket=self.bucket, Key=target_key, Body=buffer)
            logger.info(f"Uploaded {file_name} to s3://{self.bucket}/{target_key}")
        except ClientError as e:
            logger.error(f"AWS upload failed: {e}")
            raise

    def ingest(self, file_path: str, schema_path: str = None):
        try:
            logger.info(f"Starting batch ingestion for {file_path}")
            df = self._read_file(file_path)

            # Validate schema if provided
            if schema_path:
                validator = SchemaValidator(schema_path)
                df = validator.validate(df)

            file_name = os.path.basename(file_path).replace(" ", "_")
            self._upload_to_s3(df, file_name)

            logger.info("Batch ingestion completed successfully.")
        except Exception as e:
            logger.exception(f"Batch ingestion failed: {e}")
            sys.exit(1)


if __name__ == "__main__":
    ROOT_DIR = pathlib.Path(__file__).parents[1]
    aws_config = ROOT_DIR / "config" / "aws_config.yaml"
    pipeline_config = ROOT_DIR / "config" / "pipeline_config.yaml"

    # Example run
    sample_file = str(ROOT_DIR / "sample_data" / "input.csv")
    schema_file = str(ROOT_DIR / "sample_data" / "schema.json")

    ingestor = BatchIngest(str(aws_config), str(pipeline_config))
    ingestor.ingest(sample_file, schema_file)
