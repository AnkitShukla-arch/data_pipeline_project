import os
import subprocess
import logging
import yaml

# Load pipeline config
with open("config/pipeline_config.yaml", "r") as f:
    pipeline_cfg = yaml.safe_load(f)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TransformRunner")

class TransformRunner:
    def __init__(self):
        self.dbt_project_dir = pipeline_cfg["transformation"]["dbt_project_dir"]
        self.models = pipeline_cfg["transformation"]["models"]

    def run_dbt_command(self, command: str):
        """Run a dbt command inside the project directory."""
        try:
            logger.info(f"▶️ Running dbt command: {command}")
            subprocess.run(
                command,
                cwd=self.dbt_project_dir,
                shell=True,
                check=True
            )
            logger.info("✅ dbt command completed successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ dbt command failed: {e}")
            raise

    def run_full_transform(self):
        """Run dbt transformations for staging, marts, star/snowflake schemas."""
        for model in self.models:
            logger.info(f"⚡ Transforming model group: {model}")
            self.run_dbt_command(f"dbt run --select {model}")

    def run_tests(self):
        """Run dbt tests on all models."""
        logger.info("🧪 Running dbt tests...")
        self.run_dbt_command("dbt test")

if __name__ == "__main__":
    runner = TransformRunner()
    runner.run_full_transform()
    runner.run_tests()

