import json
import boto3
from kafka import KafkaConsumer
from logging_audit.audit_logger import AuditLogger
from logging_audit.error_handler import ErrorHandler
from lineage.lineage_logger import LineageLogger

class StreamIngest:
    """
    Stream ingestion for Kafka / AWS Kinesis.
    Logs events, errors, and lineage.
    """

    def __init__(self, kafka_topic: str, kafka_servers: list, s3_bucket: str):
        self.consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )
        self.s3 = boto3.client("s3")
        self.bucket = s3_bucket

        # Integrations
        self.logger = AuditLogger()
        self.error_handler = ErrorHandler()
        self.lineage_logger = LineageLogger(s3_bucket)

    @ErrorHandler.retry_on_failure
    def consume_and_store(self, prefix: str = "stream_ingest/"):
        for message in self.consumer:
            record = message.value
            key = f"{prefix}{record['id']}.json"

            # Store raw record in S3
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(record)
            )

            # Logging + lineage
            self.logger.log("stream_ingest", f"Stored record {record['id']} into {self.bucket}/{key}")
            self.lineage_logger.log_lineage(
                dataset_name="stream_data",
                operation="stream_ingest",
                source="kafka",
                target=f"s3://{self.bucket}/{key}",
                extra_info={"topic": message.topic, "partition": message.partition}
            )
