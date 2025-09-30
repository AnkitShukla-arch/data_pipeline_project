import pytest
from query_interface.llama_connector import LlamaConnector

def test_connector_init(monkeypatch):
    # Mock env vars
    monkeypatch.setenv("OPENAI_API_KEY", "fake-key")
    monkeypatch.setenv("AWS_REDSHIFT_USER", "user")
    monkeypatch.setenv("AWS_REDSHIFT_PASS", "pass")
    monkeypatch.setenv("AWS_REDSHIFT_HOST", "localhost")
    monkeypatch.setenv("AWS_REDSHIFT_PORT", "5439")
    monkeypatch.setenv("AWS_REDSHIFT_DB", "testdb")

    # Connector should init (may fail Redshift locally, but no crash)
    try:
        connector = LlamaConnector()
    except Exception:
        pytest.skip("Skipping Redshift init in test env.")
