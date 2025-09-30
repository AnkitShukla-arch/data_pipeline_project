# query_interface/llama_connector.py

import os
import yaml
import psycopg2
import logging
from llama_index import SQLDatabase, ServiceContext, VectorStoreIndex
from llama_index.llms import OpenAI
from llama_index.query_engine import NLSQLTableQueryEngine

# Load configs
CONFIG_PATH = os.path.join("config", "aws_config.yaml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

DB_CONFIG = config["redshift"]

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def get_connection():
    """
    Establish a connection to Redshift
    """
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Redshift: {e}")
        raise

def build_llama_connector():
    """
    Create an AI-powered SQL query engine with LlamaIndex
    """
    try:
        # Wrap Redshift into a SQLDatabase object
        conn = get_connection()
        sql_database = SQLDatabase(conn)

        # Use OpenAI as the LLM for query translation
        llm = OpenAI(model="gpt-4", temperature=0)

        # Context & indexing
        service_context = ServiceContext.from_defaults(llm=llm)
        index = VectorStoreIndex.from_documents([], service_context=service_context)

        # Natural language to SQL engine
        query_engine = NLSQLTableQueryEngine(sql_database=sql_database, llm=llm)

        logger.info("Llama Connector successfully initialized")
        return query_engine

    except Exception as e:
        logger.error(f"Failed to build Llama Connector: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    qe = build_llama_connector()
    question = "Show me the top 10 customers by revenue last month"
    response = qe.query(question)
    print("Natural Language Query:", question)
    print("Result:", response)

