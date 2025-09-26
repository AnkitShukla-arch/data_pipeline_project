# ml/model_registry.py
import os
import joblib
import json
from typing import Dict, Any, Optional
from datetime import datetime

from logging_audit.audit_logger import AuditLogger
from logging_audit.error_handler import ErrorHandler
from lineage.metadata_tracker import MetadataTracker

# Initialize helpers
audit_logger = AuditLogger()
error_handler = ErrorHandler()
metadata_tracker = MetadataTracker()

MODEL_DIR = "artifacts/models"
REGISTRY_FILE = os.path.join(MODEL_DIR, "registry.json")
os.makedirs(MODEL_DIR, exist_ok=True)


class ModelRegistry:
    """
    Industry-level Model Registry:
    - Stores model metadata (path, version, accuracy, timestamp)
    - Fetches latest or best-performing model
    - Ensures lineage tracking for reproducibility
    """

    def __init__(self):
        self.registry_path = REGISTRY_FILE
        self._initialize_registry()

    def _initialize_registry(self):
        if not os.path.exists(self.registry_path):
            with open(self.registry_path, "w") as f:
                json.dump({"models": []}, f)

    def _load_registry(self) -> Dict[str, Any]:
        with open(self.registry_path, "r") as f:
            return json.load(f)

    def _save_registry(self, data: Dict[str, Any]):
        with open(self.registry_path, "w") as f:
            json.dump(data, f, indent=4)

    @error_handler.handle_errors
    def register_model(self, model_path: str, metrics: Dict[str, Any], version: str):
        audit_logger.log_event("REGISTER_MODEL", f"Registering model version {version}")

        registry = self._load_registry()
        entry = {
            "version": version,
            "path": model_path,
            "metrics": metrics,
            "timestamp": datetime.utcnow().isoformat(),
        }
        registry["models"].append(entry)
        self._save_registry(registry)

        metadata_tracker.store_metadata(
            entity_name="ModelRegistry",
            version=version,
            attributes=entry,
        )

        return entry

    @error_handler.handle_errors
    def get_latest_model(self) -> Optional[Dict[str, Any]]:
        registry = self._load_registry()
        if not registry["models"]:
            return None
        latest = max(registry["models"], key=lambda m: m["timestamp"])
        audit_logger.log_event("FETCH_MODEL", f"Fetched latest model: {latest['version']}")
        return latest

    @error_handler.handle_errors
    def get_best_model(self, metric: str = "accuracy") -> Optional[Dict[str, Any]]:
        registry = self._load_registry()
        if not registry["models"]:
            return None
        best = max(
            registry["models"],
            key=lambda m: m["metrics"].get(metric, 0),
        )
        audit_logger.log_event("FETCH_MODEL", f"Fetched best model: {best['version']}")
        return best

    @error_handler.handle_errors
    def load_model(self, version: Optional[str] = None, best: bool = False):
        if best:
            model_entry = self.get_best_model()
        elif version:
            registry = self._load_registry()
            model_entry = next((m for m in registry["models"] if m["version"] == version), None)
        else:
            model_entry = self.get_latest_model()

        if not model_entry:
            raise RuntimeError("No model found in registry")

        model = joblib.load(model_entry["path"])
        audit_logger.log_event("LOAD_MODEL", f"Loaded model {model_entry['version']}")
        return model, model_entry


if __name__ == "__main__":
    registry = ModelRegistry()

    # Example usage
    model_entry = registry.register_model(
        model_path="artifacts/models/model_20250926123000.pkl",
        metrics={"accuracy": 0.92},
        version="20250926123000"
    )

    latest_model = registry.get_latest_model()
    print("Latest model:", latest_model)

    best_model = registry.get_best_model()
    print("Best model:", best_model)

