import logging
import yaml
import psycopg2
from psycopg2 import sql

# Load config
with open("config/aws_config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SchemaManager")

class SchemaManager:
    def __init__(self):
        self.cluster_id = config["aws"]["redshift"]["cluster_id"]
        self.database = config["aws"]["redshift"]["database"]
        self.user = config["aws"]["redshift"]["user"]
        self.password = config["aws"]["redshift"]["password"]
        self.port = config["aws"]["redshift"]["port"]
        self.conn = None

    def connect(self):
        """Connect to Redshift cluster."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
                host=f"{self.cluster_id}.redshift.amazonaws.com"
            )
            logger.info("✅ Connected to Redshift for schema management")
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            raise

    def create_star_schema(self):
        """
        Create Star Schema:
        - Fact table: sales
        - Dimensions: customers, products, time
        """
        try:
            cursor = self.conn.cursor()
            schema_queries = [
                """
                CREATE TABLE IF NOT EXISTS dim_customers (
                    customer_id INT PRIMARY KEY,
                    name VARCHAR(255),
                    region VARCHAR(100)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_products (
                    product_id INT PRIMARY KEY,
                    product_name VARCHAR(255),
                    category VARCHAR(100)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_time (
                    date_id DATE PRIMARY KEY,
                    day INT,
                    month INT,
                    year INT
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS fact_sales (
                    sales_id BIGINT IDENTITY PRIMARY KEY,
                    customer_id INT REFERENCES dim_customers(customer_id),
                    product_id INT REFERENCES dim_products(product_id),
                    date_id DATE REFERENCES dim_time(date_id),
                    sales_amount DECIMAL(10,2)
                );
                """
            ]
            for q in schema_queries:
                cursor.execute(q)
            self.conn.commit()
            cursor.close()
            logger.info("✅ Star Schema created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create Star Schema: {e}")
            raise

    def create_snowflake_schema(self):
        """
        Create Snowflake Schema:
        - Break down product/category into sub-dimensions
        """
        try:
            cursor = self.conn.cursor()
            schema_queries = [
                """
                CREATE TABLE IF NOT EXISTS dim_category (
                    category_id INT PRIMARY KEY,
                    category_name VARCHAR(100)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_products (
                    product_id INT PRIMARY KEY,
                    product_name VARCHAR(255),
                    category_id INT REFERENCES dim_category(category_id)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_customers (
                    customer_id INT PRIMARY KEY,
                    name VARCHAR(255),
                    region VARCHAR(100)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS dim_time (
                    date_id DATE PRIMARY KEY,
                    day INT,
                    month INT,
                    year INT
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS fact_sales (
                    sales_id BIGINT IDENTITY PRIMARY KEY,
                    customer_id INT REFERENCES dim_customers(customer_id),
                    product_id INT REFERENCES dim_products(product_id),
                    date_id DATE REFERENCES dim_time(date_id),
                    sales_amount DECIMAL(10,2)
                );
                """
            ]
            for q in schema_queries:
                cursor.execute(q)
            self.conn.commit()
            cursor.close()
            logger.info("✅ Snowflake Schema created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create Snowflake Schema: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("🔒 Redshift connection closed")

