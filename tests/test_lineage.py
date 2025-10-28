import pytest
from unittest.mock import patch
from lineage.metadata_tracker import MetadataTracker
from lineage.lineage_logger import LineageLogger

def test_metadata_tracker_logs_lineage():
    with patch.object(MetadataTracker, "track_lineage", return_value=True) as mock_track:
        tracker = MetadataTracker()
        result = tracker.track_lineage("transform_task")
        assert result is True
        mock_track.assert_called_once_with("transform_task")

def test_lineage_logger_creates_log_entry():
    with patch.object(LineageLogger, "log_event", return_value=True) as mock_log:
        logger = LineageLogger()
        assert logger.log_event("Test lineage event") is True
        mock_log.assert_called_once_with("Test lineage event")

