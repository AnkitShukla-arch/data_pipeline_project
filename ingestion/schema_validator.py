import jsonschema
from logging_audit.audit_logger import AuditLogger
from logging_audit.error_handler import ErrorHandler

class SchemaValidator:
    """
    Validates incoming dataset schema before processing.
    """

    def __init__(self, schema: dict):
        self.schema = schema
        self.logger = AuditLogger()
        self.error_handler = ErrorHandler()

    @ErrorHandler.retry_on_failure
    def validate(self, record: dict) -> bool:
        try:
            jsonschema.validate(instance=record, schema=self.schema)
            self.logger.log("schema_validator", f"Record {record} validated successfully.")
            return True
        except jsonschema.ValidationError as e:
            self.logger.log("schema_validator", f"Schema validation failed: {e.message}")
            return False
