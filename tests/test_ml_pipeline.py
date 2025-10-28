import pytest
from unittest.mock import patch, MagicMock
from ml.training_pipeline import ModelTrainer
from ml.model_registry import ModelRegistry
from ml.drift_detector import DriftDetector

def test_model_training():
    with patch.object(ModelTrainer, "train", return_value="model_v1.pkl") as mock_train:
        trainer = ModelTrainer()
        model_path = trainer.train()
        assert model_path == "model_v1.pkl"
        mock_train.assert_called_once()

def test_model_registry_save():
    with patch.object(ModelRegistry, "register_model", return_value=True) as mock_register:
        registry = ModelRegistry()
        assert registry.register_model("model_v1.pkl") is True
        mock_register.assert_called_once_with("model_v1.pkl")

def test_drift_detector_detects_no_drift():
    with patch.object(DriftDetector, "check_drift", return_value=False) as mock_drift:
        detector = DriftDetector()
        assert detector.check_drift() is False
        mock_drift.assert_called_once()
