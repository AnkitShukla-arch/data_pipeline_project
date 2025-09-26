# ml/training_pipeline.py
import os
import joblib
import uuid
from datetime import datetime

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

from logging_audit.audit_logger import AuditLogger
from logging_audit.error_handler import ErrorHandler
from lineage.metadata_tracker import MetadataTracker
from ml.model_registry import ModelRegistry

# Initialize helpers
audit_logger = AuditLogger()
error_handler = ErrorHandler()
metadata_tracker = MetadataTracker()
model_registry = ModelRegistry()

MODEL_DIR = "artifacts/models"
os.makedirs(MODEL_DIR, exist_ok=True)


class TrainingPipeline:
    """
    Industry-level Training Pipeline:
    - Trains ML models
    - Tracks lineage + metadata
    - Registers models in ModelRegistry
    """

    def __init__(self, data: pd.DataFrame, target_col: str):
        self.data = data
        self.target_col = target_col

    @error_handler.handle_errors
    def run(self):
        audit_logger.log_event("TRAINING_START", f"Training pipeline started with target: {self.target_col}")

        # Split dataset
        X = self.data.drop(columns=[self.target_col])
        y = self.data[self.target_col]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Train model
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)

        audit_logger.log_event("TRAINING_COMPLETE", f"Model trained with accuracy: {acc:.4f}")

        # Save model
        version = datetime.utcnow().strftime("%Y%m%d%H%M%S") + "_" + str(uuid.uuid4())[:8]
        model_path = os.path.join(MODEL_DIR, f"model_{version}.pkl")
        joblib.dump(model, model_path)

        # Metadata
        metadata = {
            "version": version,
            "model_type": "RandomForestClassifier",
            "features": list(X.columns),
            "target": self.target_col,
            "accuracy": acc,
            "timestamp": datetime.utcnow().isoformat(),
        }

        metadata_tracker.store_metadata(
            entity_name="TrainingPipeline",
            version=version,
            attributes=metadata,
        )

        # Register model in registry
        registry_entry = model_registry.register_model(
            model_path=model_path,
            metrics={"accuracy": acc},
            version=version,
        )

        audit_logger.log_event("MODEL_REGISTERED", f"Model {version} registered successfully.")

        return model, registry_entry


if __name__ == "__main__":
    # Example synthetic dataset
    df = pd.DataFrame({
        "feature1": [1, 2, 3, 4, 5, 6, 7, 8],
        "feature2": [5, 6, 7, 8, 9, 10, 11, 12],
        "target":   [0, 1, 0, 1, 0, 1, 0, 1]
    })

    pipeline = TrainingPipeline(df, target_col="target")
    model, registry_entry = pipeline.run()

    print("Model trained and registered:", registry_entry)
