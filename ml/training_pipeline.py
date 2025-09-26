# ml/training_pipeline.py
import os
import joblib
import pandas as pd
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score

from logging_audit.audit_logger import AuditLogger
from logging_audit.error_handler import ErrorHandler
from lineage.metadata_tracker import MetadataTracker

# Initialize helpers
audit_logger = AuditLogger()
error_handler = ErrorHandler()
metadata_tracker = MetadataTracker()

MODEL_DIR = "artifacts/models"
os.makedirs(MODEL_DIR, exist_ok=True)


class TrainingPipeline:
    """
    Industry-level ML training pipeline:
    - Reads ingested & transformed data
    - Trains model
    - Logs audit + errors
    - Stores metadata/lineage
    """

    def __init__(self, data_path: str, target_column: str):
        self.data_path = data_path
        self.target_column = target_column
        self.model = None
        self.model_version = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    @error_handler.handle_errors
    def load_data(self):
        audit_logger.log_event("LOAD_DATA", f"Loading data from {self.data_path}")
        df = pd.read_csv(self.data_path)
        if self.target_column not in df.columns:
            raise ValueError(f"Target column '{self.target_column}' not in dataset")
        return df

    @error_handler.handle_errors
    def preprocess(self, df: pd.DataFrame):
        audit_logger.log_event("PREPROCESS", "Splitting train/test data")
        X = df.drop(columns=[self.target_column])
        y = df[self.target_column]
        return train_test_split(X, y, test_size=0.2, random_state=42)

    @error_handler.handle_errors
    def train_model(self, X_train, y_train):
        audit_logger.log_event("TRAIN_MODEL", "Training RandomForestClassifier")
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        self.model = model
        return model

    @error_handler.handle_errors
    def evaluate_model(self, model, X_test, y_test):
        audit_logger.log_event("EVALUATE_MODEL", "Evaluating model performance")
        y_pred = model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        report = classification_report(y_test, y_pred, output_dict=True)

        audit_logger.log_event("EVALUATION_REPORT", f"Accuracy: {acc:.4f}")
        metadata_tracker.store_metadata(
            entity_name="Model",
            version=self.model_version,
            attributes={
                "accuracy": acc,
                "evaluation_report": report,
                "trained_on": datetime.utcnow().isoformat(),
            },
        )
        return acc, report

    @error_handler.handle_errors
    def save_model(self):
        if self.model is None:
            raise RuntimeError("No model trained to save")

        model_path = os.path.join(MODEL_DIR, f"model_{self.model_version}.pkl")
        joblib.dump(self.model, model_path)

        audit_logger.log_event("SAVE_MODEL", f"Model saved at {model_path}")
        metadata_tracker.store_metadata(
            entity_name="Model",
            version=self.model_version,
            attributes={"path": model_path},
        )
        return model_path

    def run(self):
        df = self.load_data()
        X_train, X_test, y_train, y_test = self.preprocess(df)
        model = self.train_model(X_train, y_train)
        self.evaluate_model(model, X_test, y_test)
        model_path = self.save_model()
        return model_path


if __name__ == "__main__":
    pipeline = TrainingPipeline(data_path="data/processed/clean_data.csv", target_column="target")
    model_path = pipeline.run()
    print(f"✅ Training complete. Model stored at: {model_path}")

