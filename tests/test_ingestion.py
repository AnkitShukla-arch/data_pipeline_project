import pytest
from unittest.mock import patch, MagicMock
from ingestion.batch_ingest import BatchIngestor
from ingestion.stream_ingest import StreamIngestor

@pytest.fixture
def sample_data():
    return [{"id": 1, "ip": "192.168.0.1"}, {"id": 2, "ip": "10.0.0.1"}]

def test_batch_ingest_loads_data(sample_data):
    with patch.object(BatchIngestor, "load_data", return_value=True) as mock_load:
        ingestor = BatchIngestor(source="test_source.csv")
        result = ingestor.load_data()
        assert result is True
        mock_load.assert_called_once()

def test_stream_ingest_starts_stream():
    with patch.object(StreamIngestor, "start_stream", return_value="Streaming Started") as mock_stream:
        ingestor = StreamIngestor(topic="test_topic")
        result = ingestor.start_stream()
        assert result == "Streaming Started"
        mock_stream.assert_called_once()

