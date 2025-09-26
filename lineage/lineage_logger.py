import logging
import yaml
from datetime import datetime

# Load config
with open("config/logging_config.yaml", "r") as f:
    log_cfg = yaml.safe_load(f)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LineageLogger")

class LineageLogger:
    def __init__(self):
        self.log_file = log_cfg["lineage"]["log_file"]

    def log_step(self, step_name: str, input_sources: list, output_targets: list, status: str):
        """Log lineage step (for audits + visual DAGs)."""
        lineage_entry = {
            "step": step_name,
            "inputs": input_sources,
            "outputs": output_targets,
            "status": status,
            "timestamp": datetime.utcnow().isoformat()
        }
        logger.info(f"[LINEAGE] {lineage_entry}")

        with open(self.log_file, "a") as f:
            f.write(str(lineage_entry) + "\n")

