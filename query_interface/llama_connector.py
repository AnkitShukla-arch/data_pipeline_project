import os
import logging
from sqlalchemy.engine import create_engine
from llama_index.core import SQLDatabase, ServiceContext
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.llms.openai import OpenAI

# ========================
# Logging
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

class LlamaConnector:
    def __init__(self):
        # Load configs from env
        self.db_user = os.getenv("AWS_REDSHIFT_USER", "admin")
        self.db_pass = os.getenv("AWS_REDSHIFT_PASS", "password")
        self.db_host = os.getenv("AWS_REDSHIFT_HOST", "localhost")
        self.db_port = os.getenv("AWS_REDSHIFT_PORT", "5439")
        self.db_name = os.getenv("AWS_REDSHIFT_DB", "analytics")
        self.openai_key = os.getenv("OPENAI_API_KEY")

        if not self.openai_key:
            raise RuntimeError("OPENAI_API_KEY is missing.")

        # Init Redshift connection
        conn_uri = (
            f"redshift+psycopg2://{self.db_user}:{self.db_pass}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )
        try:
            self.engine = create_engine(conn_uri)
            self.sql_db = SQLDatabase(self.engine)
            logger.info("✅ Connected to Redshift.")
        except Exception as e:
            logger.error(f"❌ Redshift connection failed: {e}")
            raise

        # Init LLM + LlamaIndex
        llm = OpenAI(api_key=self.openai_key, model="gpt-4")
        service_context = ServiceContext.from_defaults(llm=llm)

        try:
            self.query_engine = NLSQLTableQueryEngine(
                sql_database=self.sql_db,
                service_context=service_context,
                tables=None,  # all tables
                synthesize_response=True,
            )
            logger.info("✅ LlamaIndex query engine ready.")
        except Exception as e:
            logger.error(f"❌ Failed to init query engine: {e}")
            raise

    def run_query(self, question: str) -> str:
        """Run a natural language query against Redshift via LlamaIndex."""
        try:
            logger.info(f"📥 Query: {question}")
            response = self.query_engine.query(question)
            return str(response)
        except Exception as e:
            logger.error(f"❌ Query failed: {e}")
            raise
