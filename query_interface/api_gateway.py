import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.engine import create_engine
from llama_index.core import SQLDatabase, VectorStoreIndex, ServiceContext
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.llms.openai import OpenAI

# ========================
# Logging setup
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

# ========================
# FastAPI init
# ========================
app = FastAPI(title="LlamaIndex Query API", version="1.0")

# ========================
# Request schema
# ========================
class QueryRequest(BaseModel):
    question: str

# ========================
# Load configs (from env vars)
# ========================
DB_USER = os.getenv("AWS_REDSHIFT_USER", "admin")
DB_PASS = os.getenv("AWS_REDSHIFT_PASS", "password")
DB_HOST = os.getenv("AWS_REDSHIFT_HOST", "localhost")
DB_PORT = os.getenv("AWS_REDSHIFT_PORT", "5439")
DB_NAME = os.getenv("AWS_REDSHIFT_DB", "analytics")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is missing in environment variables.")

# ========================
# Database connection
# ========================
try:
    connection_uri = (
        f"redshift+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    engine = create_engine(connection_uri)
    sql_database = SQLDatabase(engine)
    logger.info("✅ Connected to AWS Redshift successfully.")
except Exception as e:
    logger.error(f"❌ Failed to connect to Redshift: {e}")
    raise

# ========================
# LlamaIndex setup
# ========================
llm = OpenAI(api_key=OPENAI_API_KEY, model="gpt-4")
service_context = ServiceContext.from_defaults(llm=llm)

try:
    query_engine = NLSQLTableQueryEngine(
        sql_database=sql_database,
        service_context=service_context,
        tables=None,  # Use all tables by default
        synthesize_response=True
    )
    logger.info("✅ LlamaIndex query engine initialized.")
except Exception as e:
    logger.error(f"❌ Failed to initialize query engine: {e}")
    raise

# ========================
# API endpoint
# ========================
@app.post("/nl_query")
async def natural_language_query(req: QueryRequest):
    try:
        logger.info(f"📥 Received query: {req.question}")
        response = query_engine.query(req.question)
        return {
            "question": req.question,
            "answer": str(response)
        }
    except Exception as e:
        logger.error(f"❌ Query failed: {e}")
        raise HTTPException(status_code=500, detail="Query execution failed.")
