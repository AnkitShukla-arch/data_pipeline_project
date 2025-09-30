
# query_interface/api_gateway.py

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import psycopg2
import yaml
import logging
import os

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

# FastAPI app
app = FastAPI(
    title="Data Pipeline Query API",
    description="Industry-level API Gateway for querying Redshift Warehouse",
    version="1.0.0"
)

class QueryRequest(BaseModel):
    sql: str

def get_connection():
    try:
        return psycopg2.connect(
            dbname=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
        )
    except Exception as e:
        logger.error(f"DB Connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

@app.post("/query")
def run_query(request: QueryRequest):
    """
    Run a custom SQL query on the warehouse.
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(request.sql)
        
        # Try to fetch results (only for SELECT)
        if request.sql.strip().lower().startswith("select"):
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            result = [dict(zip(cols, row)) for row in rows]
        else:
            conn.commit()
            result = {"message": "Query executed successfully"}
        
        cur.close()
        return {"status": "success", "data": result}

    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    finally:
        if conn:
            conn.close()

@app.get("/health")
def health_check():
    """
    Health check endpoint for monitoring.
    """
    return {"status": "ok"}
