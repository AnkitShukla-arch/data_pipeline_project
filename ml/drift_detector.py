import os
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score
from scipy.stats import ks_2samp
from datetime import datetime
from logging_audit.audit_logger import get_logger
from logging_audit.error_handler import ErrorHandler
from lineage.metadata_tracker import MetadataTracker
from ml.training_pipeline import train_and_register_model

logger = get_logger(__name__)
error_handler = ErrorHandler()
metadata_tracker = MetadataTracker()

ARTIFACTS_DIR = "artifacts"
MODELS_DIR = os.path.join(ARTIFACTS_DIR, "models")
REGISTRY_FILE = os.path.join(ARTIFACTS_DIR, "registry", "registry.json")


class DriftDetector:
    def __init__(self, threshold=0.05, accuracy_drop=0.1):
        """
        :param threshold: p-value threshold for data drift (Kolmogorov-Smirnov test)
        :param accuracy_drop: acceptable drop in accuracy before retraining
        """
        self.threshold = threshold
        self.accuracy_drop = accuracy_drop

    def _load_latest_model(self):
        if not os.path.exists(REGISTRY_FILE):
            logger.warning("No registry found, training first model.")
            return None, None

        with open(REGISTRY_FILE, "r") as f:
            registry = json.load(f)

        if not registry["models"]:
            logger.warning("Registry empty, training first model.")
            return None, None

        latest_model = registry["models"][-1]
        model_path = latest_model["path"]
        return joblib.load(model_path), latest_model

    def check_data_drift(self, reference_data, new_data):
        drifted_features = []
        for col in reference_data.columns:
            if col not in new_data.columns:
                continue
            stat, p_value = ks_2samp(reference_data[col], new_data[col])
            if p_value < self.threshold:
                drifted_features.append(col)
        return drifted_features

    def check_model_drift(self, model, X_ref, y_ref, X_new, y_new):
        ref_acc = accuracy_score(y_ref, model.predict(X_ref))
        new_acc = accuracy_score(y_new, model.predict(X_new))
        drift = (ref_acc - new_acc) > self.accuracy_drop
        return drift, ref_acc, new_acc

    @error_handler.handle_errors
    def monitor_and_retrain(self, reference_data, new_data, target_col="target"):
        model, latest_meta = self._load_latest_model()

        if model is None:
            logger.info("No model found. Training initial model.")
            train_and_register_model(reference_data, target_col)
            return

        # Split
        X_ref, y_ref = reference_data.drop(columns=[target_col]), reference_data[target_col]
        X_new, y_new = new_data.drop(columns=[target_col]), new_data[target_col]

        # Data drift
        drifted = self.check_data_drift(X_ref, X_new)
        if drifted:
            logger.warning(f"Data drift detected in features: {drifted}")

        # Model drift
        model_drift, ref_acc, new_acc = self.check_model_drift(model, X_ref, y_ref, X_new, y_new)
        if model_drift:
            logger.warning(f"Model drift detected: ref_acc={ref_acc}, new_acc={new_acc}")

        if drifted or model_drift:
            logger.info("Retraining model due to drift...")
            train_and_register_model(pd.concat([reference_data, new_data]), target_col)
            metadata_tracker.track("model_retrain", {
                "drifted_features": drifted,
                "ref_accuracy": ref_acc,
                "new_accuracy": new_acc,
                "retrain_time": datetime.utcnow().isoformat()
            })
        else:
            logger.info("No drift detected. Model is stable.")


if __name__ == "__main__":
    # Example run
    ref = pd.DataFrame({
        "feature1": np.random.normal(0, 1, 1000),
        "feature2": np.random.normal(5, 2, 1000),
        "target": np.random.choice([0, 1], size=1000)
    })

    new = pd.DataFrame({
        "feature1": np.random.normal(1, 1, 1000),  # shifted mean → drift
        "feature2": np.random.normal(5, 2, 1000),
        "target": np.random.choice([0, 1], size=1000)
    })

    detector = DriftDetector()
    detector.monitor_and_retrain(ref, new, target_col="target")

