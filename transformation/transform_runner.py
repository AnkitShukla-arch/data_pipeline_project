import subprocess
import os
from logging_audit.audit_logger import AuditLogger
from logging_audit.error_handler import ErrorHandler
from lineage.lineage_logger import LineageLogger

class TransformRunner:
    """
    Runs dbt transformations (staging, star, snowflake).
    """

    def __init__(self, project_dir: str, profiles_dir: str, s3_bucket: str):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.logger = AuditLogger()
        self.lineage_logger = LineageLogger(s3_bucket)

    @ErrorHandler.retry_on_failure
    def run(self, target: str = "dev"):
        self.logger.log("transform_runner", f"Starting dbt run for target={target}")
        
        try:
            subprocess.run(
                ["dbt", "run", f"--profiles-dir={self.profiles_dir}", f"--project-dir={self.project_dir}", f"--target={target}"],
                check=True
            )
            self.logger.log("transform_runner", "dbt run completed successfully.")

            self.lineage_logger.log_lineage(
                dataset_name="warehouse_models",
                operation="transform",
                source="staging",
                target="analytics",
                extra_info={"dbt_project": self.project_dir, "target": target}
            )

        except subprocess.CalledProcessError as e:
            self.logger.log("transform_runner", f"dbt run failed: {str(e)}")
            raise
