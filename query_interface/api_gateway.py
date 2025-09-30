from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from query_interface.llama_connector import LlamaConnector
import logging

# ========================
# Logging
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

# ========================
# FastAPI app + request model
# ========================
app = FastAPI(title="LlamaIndex Query API")

class QueryRequest(BaseModel):
    question: str

# ========================
# Init connector once
# ========================
try:
    llama_connector = LlamaConnector()
    logger.info("✅ LlamaConnector initialized in API.")
except Exception as e:
    logger.error(f"❌ Failed to init LlamaConnector in API: {e}")
    llama_connector = None


# ========================
# Routes
# ========================
@app.get("/")
def root():
    return {"message": "Welcome to the LlamaIndex Query API 🚀"}


@app.post("/query")
def run_query(req: QueryRequest):
    """Run NL query against Redshift via LlamaIndex."""
    if not llama_connector:
        raise HTTPException(status_code=500, detail="LlamaConnector not available.")
    try:
        result = llama_connector.run_query(req.question)
        return {"query": req.question, "result": result}
    except Exception as e:
        logger.error(f"❌ Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
