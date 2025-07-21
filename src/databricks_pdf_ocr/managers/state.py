"""State manager for tracking processing state."""

import json
import uuid
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

from ..config import OCRProcessingConfig
from ..schemas import get_state_schema


class StateManager:
    """Manages processing state and run tracking."""

    def __init__(self, spark: SparkSession, config: OCRProcessingConfig):
        self.spark = spark
        self.config = config

    def create_run_record(self, run_config: dict[str, Any]) -> str:
        """Create a new run record and return run_id."""
        run_id = str(uuid.uuid4())

        run_data = [
            {
                "run_id": run_id,
                "run_timestamp": datetime.now(),
                "processing_mode": self.config.processing_mode,
                "files_processed": 0,
                "files_succeeded": 0,
                "files_failed": 0,
                "total_pages_processed": 0,
                "processing_duration_seconds": 0.0,
                "configuration": json.dumps(run_config),
            }
        ]

        # Use explicit schema to ensure consistent data types
        run_df = self.spark.createDataFrame(run_data, schema=get_state_schema())
        run_df.write.mode("append").saveAsTable(self.config.state_table_path)

        return run_id

    def update_run_record(
        self, run_id: str, stats: dict[str, Any], duration_seconds: float
    ) -> None:
        """Update run record with final statistics."""
        # Use DataFrame operations to prevent SQL injection
        state_df = self.spark.table(self.config.state_table_path)

        # Create update DataFrame with the new values
        update_values = {
            "files_processed": lit(stats["files_processed"]),
            "files_succeeded": lit(stats["files_succeeded"]),
            "files_failed": lit(stats["files_failed"]),
            "total_pages_processed": lit(stats["total_pages_processed"]),
            "processing_duration_seconds": lit(duration_seconds),
        }

        # For now use a temporary approach with overwrite mode
        # Note: In production, consider using Delta Lake MERGE operations
        updated_df = state_df.withColumn("temp_run_id", col("run_id"))
        for column, value in update_values.items():
            updated_df = updated_df.withColumn(
                column, when(col("run_id") == run_id, value).otherwise(col(column))
            )

        # Write back the updated table
        updated_df.drop("temp_run_id").write.mode("overwrite").saveAsTable(
            self.config.state_table_path
        )

    def get_last_successful_run(self) -> dict[str, Any]:
        """Get information about the last successful run."""
        try:
            last_run = (
                self.spark.table(self.config.state_table_path)
                .filter("files_processed > 0")
                .orderBy("run_timestamp", ascending=False)
                .limit(1)
                .collect()
            )

            if last_run:
                row = last_run[0]
                return {
                    "run_id": row.run_id,
                    "run_timestamp": row.run_timestamp,
                    "processing_mode": row.processing_mode,
                    "files_processed": row.files_processed,
                    "files_succeeded": row.files_succeeded,
                    "files_failed": row.files_failed,
                    "total_pages_processed": row.total_pages_processed,
                    "processing_duration_seconds": row.processing_duration_seconds,
                    "configuration": json.loads(row.configuration),
                }
            else:
                return {}

        except Exception:
            # State table doesn't exist yet
            return {}

    def get_processing_history(self, limit: int = 10) -> list:
        """Get processing history."""
        try:
            history = (
                self.spark.table(self.config.state_table_path)
                .orderBy("run_timestamp", ascending=False)
                .limit(limit)
                .collect()
            )

            return [
                {
                    "run_id": row.run_id,
                    "run_timestamp": row.run_timestamp,
                    "processing_mode": row.processing_mode,
                    "files_processed": row.files_processed,
                    "files_succeeded": row.files_succeeded,
                    "files_failed": row.files_failed,
                    "total_pages_processed": row.total_pages_processed,
                    "processing_duration_seconds": row.processing_duration_seconds,
                }
                for row in history
            ]

        except Exception:
            # State table doesn't exist yet
            return []
