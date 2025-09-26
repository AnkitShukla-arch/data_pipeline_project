import pandas as pd
from logging_audit.audit_logger import log_info, log_error


def validate_schema(df: pd.DataFrame, expected_schema: dict) -> bool:
    """
    Validate dataframe against expected schema.
    Example schema:
        {"id": "int64", "name": "object", "age": "int64"}
    """

    for col, dtype in expected_schema.items():
        if col not in df.columns:
            log_error(f"Missing column: {col}")
            return False
        if str(df[col].dtype) != dtype:
            log_error(f"Column {col} expected {dtype}, got {df[col].dtype}")
            return False

    log_info("Schema validation passed ✅")
    return True

