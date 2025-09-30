# data_pipeline_project/ml/drift_detector.py
import os
import sys
import json
import logging
import joblib
import numpy as np
import pandas as pd
from typing import Dict, Any
from scipy.stats import ks_2samp
from sklearn.metrics import accuracy_score

# Directories
ARTIFACTS_DIR = os.path.join("artifacts")
MODEL_DIR = os.path.join(ARTIFACTS_DIR, "models")
DRIFT_DIR = os.path.join(ARTIFACTS_DIR, "drift_reports")

os.makedirs(DRIFT_DIR, exist_ok=True)

logger = logging.getLogger("drift_detector")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def detect_data_drift(
    baseline_data: pd.DataFrame,
    new_data: pd.DataFrame,
    threshold: float = 0.05
) -> Dict[str, Any]:
    """
    Detects data drift using Kolmogorov-Smirnov test.
    """
    drift_report = {"data_drift": False, "details": {}}

    for col in baseline_data.columns:
        if col not in new_data.columns:
            continue
        try:
            stat, p_value = ks_2samp(
                baseline_data[col].dropna(), new_data[col].dropna()
            )
            drift_report["details"][col] = {"p_value": float(p_value)}
            if p_value < threshold:
                drift_report["data_drift"] = True
                drift_report["details"][col]["drift_detected"] = True
            else:
                drift_report["details"][col]["drift_detected"] = False
        except Exception as e:
            drift_report["details"][col] = {"error": str(e)}

    return drift_report


def detect_model_drift(
    model_path: str,
    baseline_data: pd.DataFrame,
    new_data: pd.DataFrame,
    target_col: str
) -> Dict[str, Any]:
    """
    Detects model drift by comparing accuracy on baseline vs. new data.
    """
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")

    model = joblib.load(model_path)

    baseline_X = baseline_data.drop(columns=[target_col])
    baseline_y = baseline_data[target_col]
    new_X = new_data.drop(columns=[target_col])
    new_y = new_data[target_col]

    baseline_acc = accuracy_score(baseline_y, model.predict(baseline_X))
    new_acc = accuracy_score(new_y, model.predict(new_X))

    drift_detected = new_acc < (baseline_acc - 0.1)  # 10% drop tolerance

    return {
        "model_drift": drift_detected,
        "baseline_accuracy": float(baseline_acc),
        "new_accuracy": float(new_acc),
        "threshold": 0.1,
    }


def run_drift_check(
    baseline_csv: str,
    new_csv: str,
    model_path: str,
    target_col: str
) -> Dict[str, Any]:
    """
    Runs both data drift and model drift detection, saves JSON report.
    """
    baseline_data = pd.read_csv(baseline_csv)
    new_data = pd.read_csv(new_csv)

    data_drift_report = detect_data_drift(baseline_data, new_data)
    model_drift_report = detect_model_drift(model_path, baseline_data, new_data, target_col)

    report = {
        "data_drift": data_drift_report,
        "model_drift": model_drift_report,
    }

    report_path = os.path.join(
        DRIFT_DIR, f"drift_report_{os.path.basename(model_path)}.json"
    )
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Drift report saved at {report_path}")
    return report


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python ml/drift_detector.py <baseline_csv> <new_csv> <model_path> [target_col]")
        sys.exit(1)

    baseline_csv = sys.argv[1]
    new_csv = sys.argv[2]
    model_path = sys.argv[3]
    target_col = sys.argv[4] if len(sys.argv) > 4 else "target"

    try:
        report = run_drift_check(baseline_csv, new_csv, model_path, target_col)
        print("DRIFT CHECK REPORT:", json.dumps(report, indent=2))
    except Exception as e:
        logger.exception(f"Drift check failed: {e}")
        sys.exit(1)
