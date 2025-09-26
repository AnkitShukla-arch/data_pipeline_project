import os
import logging
import yaml
import psycopg2
from psycopg2 import sql

# Load AWS/Redshift config
with open("config/aws_config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RedshiftLoader")

class RedshiftLoader:
    def __init__(self):
        self.cluster_id = config["aws"]["redshift"]["cluster_id"]
        self.database = config["aws"]["redshift"]["database"]
        self.user = config["aws"]["redshift"]["user"]
        self.password = config["aws"]["redshift"]["password"]
        self.port = config["aws"]["redshift"]["port"]
        self.conn = None

    def connect(self):
        """Establish connection to Redshift cluster."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
                host=f"{self.cluster_id}.redshift.amazonaws.com"
            )
            logger.info("✅ Connected to Redshift successfully")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redshift: {e}")
            raise

    def copy_from_s3(self, table_name: str, s3_path: str, iam_role: str, file_format: str = "CSV"):
        """Load data from S3 into Redshift using COPY command."""
        try:
            if not self.conn:
                self.connect()

            cursor = self.conn.cursor()
            copy_query = sql.SQL("""
                COPY {table}
                FROM %s
                IAM_ROLE %s
                FORMAT AS {fmt}
                IGNOREHEADER 1
                REGION 'us-east-1';
            """).format(
                table=sql.Identifier(table_name),
                fmt=sql.SQL(file_format)
            )

            cursor.execute(copy_query, (s3_path, iam_role))
            self.conn.commit()
            cursor.close()
            logger.info(f"✅ Data loaded into {table_name} from {s3_path}")
        except Exception as e:
            logger.error(f"❌ Failed to load data into {table_name}: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("🔒 Connection to Redshift closed")

