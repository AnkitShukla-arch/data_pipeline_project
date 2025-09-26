import logging
import json
import boto3
import yaml
from datetime import datetime

# Load config
with open("config/aws_config.yaml", "r") as f:
    aws_cfg = yaml.safe_load(f)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MetadataTracker")

class MetadataTracker:
    def __init__(self):
        self.glue = boto3.client(
            "glue",
            region_name=aws_cfg["aws"]["region"],
            aws_access_key_id=aws_cfg["aws"]["access_key"],
            aws_secret_access_key=aws_cfg["aws"]["secret_key"]
        )

    def register_table(self, database: str, table_name: str, schema: dict):
        """Register or update a table in Glue Data Catalog."""
        try:
            self.glue.create_table(
                DatabaseName=database,
                TableInput={
                    "Name": table_name,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": col, "Type": dtype} for col, dtype in schema.items()
                        ],
                        "Location": f"s3://{aws_cfg['aws']['s3_bucket']}/{database}/{table_name}/",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"},
                    },
                    "TableType": "EXTERNAL_TABLE",
                },
            )
            logger.info(f"✅ Registered table {table_name} in Glue Data Catalog")
        except self.glue.exceptions.AlreadyExistsException:
            logger.info(f"ℹ️ Table {table_name} already exists, updating metadata...")
            # update logic if needed
        except Exception as e:
            logger.error(f"❌ Failed to register table {table_name}: {e}")
            raise

    def log_pipeline_metadata(self, pipeline_name: str, status: str, details: dict):
        """Log metadata for pipeline run in JSON (can push to S3 or Atlas)."""
        metadata = {
            "pipeline": pipeline_name,
            "status": status,
            "details": details,
            "timestamp": datetime.utcnow().isoformat(),
        }
        metadata_json = json.dumps(metadata, indent=2)
        logger.info(f"[METADATA] {metadata_json}")

        # Optional → save metadata in S3
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_cfg["aws"]["access_key"],
            aws_secret_access_key=aws_cfg["aws"]["secret_key"]
        )
        s3.put_object(
            Bucket=aws_cfg["aws"]["s3_bucket"],
            Key=f"metadata/{pipeline_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
            Body=metadata_json
        )

