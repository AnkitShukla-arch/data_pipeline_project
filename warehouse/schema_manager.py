from logging_audit.audit_logger import AuditLogger
from logging_audit.error_handler import ErrorHandler

class SchemaManager:
    """
    Manages creation of star/snowflake schemas in Redshift.
    """

    def __init__(self, conn):
        self.conn = conn
        self.logger = AuditLogger()

    @ErrorHandler.retry_on_failure
    def create_star_schema(self):
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS star;
            CREATE TABLE IF NOT EXISTS star.fact_orders (
                order_id VARCHAR,
                order_date DATE,
                status VARCHAR,
                customer_name VARCHAR,
                customer_email VARCHAR
            );
        """)
        self.conn.commit()
        cursor.close()

        self.logger.log("schema_manager", "Star schema created/validated in Redshift.")

    @ErrorHandler.retry_on_failure
    def create_snowflake_schema(self):
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS snowflake;
            CREATE TABLE IF NOT EXISTS snowflake.dim_customers (
                customer_id VARCHAR,
                customer_name VARCHAR,
                customer_email VARCHAR,
                created_at TIMESTAMP
            );
        """)
        self.conn.commit()
        cursor.close()

        self.logger.log("schema_manager", "Snowflake schema created/validated in Redshift.")
