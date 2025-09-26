import psycopg2
import pandas as pd
from logging_audit.audit_logger import AuditLogger
from logging_audit.error_handler import ErrorHandler
from lineage.lineage_logger import LineageLogger

class RedshiftLoader:
    """
    Loads transformed dataframes into AWS Redshift.
    """

    def __init__(self, host, dbname, user, password, port, s3_bucket):
        self.conn_params = {
            "host": host,
            "dbname": dbname,
            "user": user,
            "password": password,
            "port": port
        }
        self.logger = AuditLogger()
        self.lineage_logger = LineageLogger(s3_bucket)

    def _get_connection(self):
        return psycopg2.connect(**self.conn_params)

    @ErrorHandler.retry_on_failure
    def load_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "public"):
        conn = self._get_connection()
        cursor = conn.cursor()

        # Create schema if not exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        # Create table dynamically (basic example, can be improved)
        cols = ", ".join([f"{col} VARCHAR" for col in df.columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({cols});")

        # Insert rows
        for _, row in df.iterrows():
            values = "', '".join(map(str, row.values))
            cursor.execute(f"INSERT INTO {schema}.{table_name} VALUES ('{values}');")

        conn.commit()
        cursor.close()
        conn.close()

        # Logging + lineage
        self.logger.log("redshift_loader", f"Loaded {len(df)} rows into {schema}.{table_name}")
        self.lineage_logger.log_lineage(
            dataset_name=table_name,
            operation="load",
            source="dbt_transforms",
            target=f"redshift.{schema}.{table_name}"
        )
