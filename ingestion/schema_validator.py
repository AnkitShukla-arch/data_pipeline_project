import json
import logging
import pandas as pd
from pydantic import BaseModel, ValidationError
from typing import List, Dict, Any

logger = logging.getLogger("schema_validator")


class SchemaValidator:
    def __init__(self, schema_path: str):
        with open(schema_path, "r") as f:
            self.schema = json.load(f)

        # Dynamically build pydantic model from schema
        fields: Dict[str, Any] = {
            col["name"]: (eval(col["type"]), ...)
            for col in self.schema["fields"]
        }
        self.Model = type("DynamicSchema", (BaseModel,), fields)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        valid_rows = []
        for _, row in df.iterrows():
            try:
                self.Model(**row.to_dict())
                valid_rows.append(row)
            except ValidationError as e:
                logger.warning(f"Row validation failed: {e}")

        logger.info(f"Validated dataframe: {len(valid_rows)} / {len(df)} rows passed")
        return pd.DataFrame(valid_rows)
