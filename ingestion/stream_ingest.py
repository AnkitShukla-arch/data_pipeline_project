import time
import os
from ingestion.batch_ingest import batch_ingest
from logging_audit.audit_logger import log_info


def stream_ingest(folder="data/", expected_schema=None, poll_interval=10):
    """
    Simulates streaming ingestion by polling a folder for new files.
    In production -> replace with Kafka / Kinesis consumer.
    """

    log_info("Starting streaming ingestion...")
    seen = set()

    while True:
        files = [f for f in os.listdir(folder) if f not in seen]
        for file in files:
            path = os.path.join(folder, file)
            batch_ingest(path, expected_schema)
            seen.add(file)
        time.sleep(poll_interval)

