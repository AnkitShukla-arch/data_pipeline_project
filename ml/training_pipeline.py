# data_pipeline_project/ml/training_pipeline.py
import os
import sys
import json
import uuid
import joblib
import logging
from datetime import datetime
from typing import Dict, Any

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

# Internal project imports (with safe fallbacks)
try:
    from logging_audit.audit_logger import AuditLogger
    audit_logger = AuditLogger()
except Exception:
    audit_logger = None

try:
    from ml.model_registry import ModelRegistry
    model_registry = ModelRegistry()
except Exception:
    model_registry = None

# Directories
ARTIFACTS_DIR = os.path.join("artifacts")
MODEL_DIR = os.path.join(ARTIFACTS_DIR, "models")
METADATA_DIR = os.path.join(ARTIFACTS_DIR, "metadata")
REGISTRY_FILE = os.path.join(MODEL_DIR, "registry.json")

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)

# Standard logger (used if AuditLogger is unavailable)
logger = logging.getLogger("training_pipeline")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def _audit(event: str, details: Dict[str, Any]):
    """Write audit logs to audit_logger if available, else fallback logger."""
    payload = {"event": event, "details": details, "ts": datetime.utcnow().isoformat()}
    try:
        if audit_logger and hasattr(audit_logger, "log_event"):
            audit_logger.log_event(event, details)
        else:
            logger.info(f"AUDIT {json.dumps(payload)}")
    except Exception as e:
        logger.warning(f"AUDIT_LOG_FAIL {event}: {e}")


def _update_local_registry(entry: Dict[str, Any]):
    """Maintain local JSON registry if no central ModelRegistry."""
    try:
        if not os.path.exists(REGISTRY_FILE):
            with open(REGISTRY_FILE, "w") as f:
                json.dump({"models": []}, f, indent=2)

        with open(REGISTRY_FILE, "r+") as f:
            data = json.load(f)
            data.setdefault("models", []).append(entry)
            f.seek(0)
            json.dump(data, f, indent=2)
            f.truncate()
    except Exception as e:
        logger.error(f"Failed to update local registry: {e}")


def _save_metadata(name: str, data: Dict[str, Any]) -> str:
    """Save training metadata to artifacts/metadata/ folder."""
    path = os.path.join(METADATA_DIR, f"{name}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    return path


def run_training_pipeline(
    data_path: str,
    target_col: str = "target",
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 100,
) -> Dict[str, Any]:
    """Train model, evaluate, register, and log metadata."""
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}")

    _audit("TRAINING_START", {"data_path": data_path, "target": target_col})

    df = pd.read_csv(data_path)
    if target_col not in df.columns:
        raise KeyError(f"Target column '{target_col}' not found in data")

    df = df.dropna(subset=[target_col])
    X, y = df.drop(columns=[target_col]), df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y if len(y.unique()) > 1 else None
    )

    model = RandomForestClassifier(n_estimators=n_estimators, random_state=random_state, n_jobs=-1)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    acc = float(accuracy_score(y_test, y_pred))
    report = classification_report(y_test, y_pred, output_dict=True)

    version = datetime.utcnow().strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:8]
    model_path = os.path.join(MODEL_DIR, f"model_{version}.pkl")
    joblib.dump(model, model_path)

    metadata = {
        "version": version,
        "model_path": model_path,
        "model_type": "RandomForestClassifier",
        "metrics": {"accuracy": acc},
        "classification_report": report,
        "train_size": len(X_train),
        "test_size": len(X_test),
        "features": list(X.columns),
        "target": target_col,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Register
    if model_registry and hasattr(model_registry, "register_model"):
        model_registry.register_model(model_path, metadata["metrics"], version)
        logger.info(f"Model registered in central registry: {version}")
    else:
        _update_local_registry(metadata)
        logger.info(f"Model registered locally in {REGISTRY_FILE}: {version}")

    # Save metadata file
    metadata_path = _save_metadata(f"model_metadata_{version}", metadata)

    _audit("TRAINING_COMPLETE", {"version": version, "accuracy": acc, "model": model_path})

    return {"model_path": model_path, "version": version, "metrics": metadata["metrics"], "metadata": metadata_path}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ml/training_pipeline.py <data_csv_path> [target_column]")
        sys.exit(1)

    csv_file = sys.argv[1]
    target_column = sys.argv[2] if len(sys.argv) > 2 else "target"

    try:
        result = run_training_pipeline(csv_file, target_column)
        print("TRAINING SUCCESS:", json.dumps(result, indent=2))
    except Exception as e:
        logger.exception(f"Training failed: {e}")
        sys.exit(1)
