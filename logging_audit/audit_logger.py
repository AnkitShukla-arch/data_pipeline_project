import os
import json
import datetime
import logging
import sys
from pythonjsonlogger import jsonlogger
from typing import Dict, Any

# --- Structured Logger Setup ---
def get_logger(name: str) -> logging.Logger:
    """
    Configure and return a JSON structured logger.
    Compatible with AWS CloudWatch, Datadog, ELK, etc.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# --- Audit Trail Manager ---
AUDIT_FILE = os.path.join("logs", "audit_log.json")

class AuditLogger:
    """
    Handles pipeline audit trail logging (who, what, when, status).
    Stores structured JSON for lineage + traceability.
    """

    def __init__(self):
        os.makedirs("logs", exist_ok=True)
        if not os.path.exists(AUDIT_FILE):
            with open(AUDIT_FILE, "w") as f:
                json.dump([], f)

    def log_event(self, event: Dict[str, Any]) -> None:
        """
        Append a structured event to the audit log.
        Example event:
        {
            "user": "system",
            "action": "ingestion",
            "dataset": "sales_2025.csv",
            "status": "SUCCESS"
        }
        """
        event["timestamp"] = datetime.datetime.utcnow().isoformat()

        with open(AUDIT_FILE, "r+") as f:
            data = json.load(f)
            data.append(event)
            f.seek(0)
            json.dump(data, f, indent=4)

    def read_audit_log(self):
        """Retrieve all logged audit events."""
        with open(AUDIT_FILE, "r") as f:
            return json.load(f)
