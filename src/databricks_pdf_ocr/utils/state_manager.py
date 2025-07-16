import json
import uuid
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from ..config.settings import OCRConfig, ProcessingMode, RunMetrics


class StateManager:
    """Manages processing state and tracks progress across runs"""

    def __init__(self, spark: SparkSession, config: OCRConfig):
        self.spark = spark
        self.config = config
        self.source_table = config.base_path + ".pdf_source"
        self.target_table = config.target_table_name
        self.state_table = config.state_table_name

    def get_pending_files(
        self,
        mode: ProcessingMode = ProcessingMode.INCREMENTAL,
        max_files: int | None = None,
        specific_ids: list[str] | None = None,
    ) -> DataFrame:
        """Get files pending processing based on mode"""

        if mode == ProcessingMode.INCREMENTAL:
            # Get files that are pending or failed with retry attempts remaining
            condition = (col("processing_status") == "pending") | (
                (col("processing_status") == "failed")
                & (col("processing_attempts") < self.config.max_retries)
            )

        elif mode == ProcessingMode.REPROCESS_ALL:
            # Get all files regardless of status
            condition = lit(True)

        elif mode == ProcessingMode.REPROCESS_SPECIFIC:
            if not specific_ids:
                raise ValueError("specific_ids must be provided for REPROCESS_SPECIFIC mode")
            # Get only specified files
            condition = col("file_id").isin(specific_ids)

        else:
            raise ValueError(f"Unknown processing mode: {mode}")

        # Build query
        query = self.spark.table(self.source_table).filter(condition)

        # Apply limit if specified
        if max_files:
            query = query.limit(max_files)

        return query.select(
            "file_id",
            "file_path",
            "file_name",
            "file_content",
            "processing_status",
            "processing_attempts",
            "last_error",
        )

    def update_file_status(
        self,
        file_id: str,
        status: str,
        error: str | None = None,
        increment_attempts: bool = True,
    ) -> None:
        """Update processing status for a file"""

        # Validate status
        valid_statuses = ["pending", "processing", "completed", "failed"]
        if status not in valid_statuses:
            raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}")

        # Build update expression
        update_expr = {
            "processing_status": lit(status),
            "last_error": lit(error) if error else col("last_error"),
        }

        if increment_attempts:
            update_expr["processing_attempts"] = col("processing_attempts") + 1

        # Update the record
        self.spark.table(self.source_table).filter(col("file_id") == file_id).select(
            "*"
        ).write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
            self.source_table + "_temp"
        )

        # Use merge to update
        merge_sql = f"""
        MERGE INTO {self.source_table} target
        USING (
            SELECT
                file_id,
                '{status}' as processing_status,
                {"NULL" if error is None else f"'{error}'"} as last_error,
                processing_attempts + {"1" if increment_attempts else "0"} as processing_attempts
            FROM {self.source_table}
            WHERE file_id = '{file_id}'
        ) source
        ON target.file_id = source.file_id
        WHEN MATCHED THEN UPDATE SET
            processing_status = source.processing_status,
            last_error = source.last_error,
            processing_attempts = source.processing_attempts
        """

        self.spark.sql(merge_sql)

    def update_multiple_file_statuses(self, file_updates: list[dict[str, Any]]) -> None:
        """Update processing status for multiple files efficiently"""

        if not file_updates:
            return

        # Create a temporary view with the updates
        update_data = []
        for update in file_updates:
            file_id = update["file_id"]
            status = update["status"]
            error = update.get("error")
            increment_attempts = update.get("increment_attempts", True)

            update_data.append(
                {
                    "file_id": file_id,
                    "new_status": status,
                    "new_error": error,
                    "increment_attempts": increment_attempts,
                }
            )

        # Convert to DataFrame
        updates_df = self.spark.createDataFrame(update_data)
        updates_df.createOrReplaceTempView("file_updates")

        # Perform bulk update using MERGE
        merge_sql = f"""
        MERGE INTO {self.source_table} target
        USING (
            SELECT
                u.file_id,
                u.new_status,
                u.new_error,
                s.processing_attempts + CASE WHEN u.increment_attempts THEN 1 ELSE 0 END as new_attempts
            FROM file_updates u
            JOIN {self.source_table} s ON u.file_id = s.file_id
        ) source
        ON target.file_id = source.file_id
        WHEN MATCHED THEN UPDATE SET
            processing_status = source.new_status,
            last_error = source.new_error,
            processing_attempts = source.new_attempts
        """

        self.spark.sql(merge_sql)

    def record_run_metrics(self, metrics: RunMetrics) -> None:
        """Record metrics for a processing run"""

        # Convert configuration to JSON string
        config_json = json.dumps(metrics.configuration, default=str)

        # Create metrics DataFrame
        metrics_data = [
            {
                "run_id": metrics.run_id,
                "run_timestamp": datetime.now(),
                "processing_mode": metrics.processing_mode.value,
                "files_processed": metrics.files_processed,
                "files_succeeded": metrics.files_succeeded,
                "files_failed": metrics.files_failed,
                "total_pages_processed": metrics.total_pages_processed,
                "processing_duration_seconds": metrics.processing_duration_seconds,
                "configuration": config_json,
            }
        ]

        metrics_df = self.spark.createDataFrame(metrics_data)

        # Insert into state table
        metrics_df.write.mode("append").saveAsTable(self.state_table)

    def get_processing_stats(self) -> dict[str, Any]:
        """Get current processing statistics"""

        # Get source table stats
        source_stats = (
            self.spark.table(self.source_table).groupBy("processing_status").count().collect()
        )
        status_counts = {row["processing_status"]: row["count"] for row in source_stats}

        # Get recent run stats
        recent_runs = (
            self.spark.table(self.state_table)
            .orderBy(col("run_timestamp").desc())
            .limit(5)
            .collect()
        )

        # Get target table stats
        target_count = self.spark.table(self.target_table).count()

        return {
            "source_file_status": status_counts,
            "extracted_pages": target_count,
            "recent_runs": [
                {
                    "run_id": row["run_id"],
                    "timestamp": row["run_timestamp"],
                    "mode": row["processing_mode"],
                    "files_processed": row["files_processed"],
                    "files_succeeded": row["files_succeeded"],
                    "files_failed": row["files_failed"],
                    "pages_processed": row["total_pages_processed"],
                    "duration_seconds": row["processing_duration_seconds"],
                }
                for row in recent_runs
            ],
        }

    def get_failed_files(self, limit: int | None = None) -> DataFrame:
        """Get files that have failed processing"""

        query = (
            self.spark.table(self.source_table)
            .filter(col("processing_status") == "failed")
            .select("file_id", "file_path", "file_name", "processing_attempts", "last_error")
            .orderBy(col("processing_attempts").desc())
        )

        if limit:
            query = query.limit(limit)

        return query

    def reset_file_status(self, file_ids: list[str]) -> None:
        """Reset processing status for specific files to enable reprocessing"""

        if not file_ids:
            return

        # Reset to pending status with zero attempts
        reset_sql = f"""
        UPDATE {self.source_table}
        SET
            processing_status = 'pending',
            processing_attempts = 0,
            last_error = NULL
        WHERE file_id IN ({",".join([f"'{fid}'" for fid in file_ids])})
        """

        self.spark.sql(reset_sql)

    def cleanup_old_runs(self, days_to_keep: int = 30) -> None:
        """Clean up old run records from the state table"""

        cleanup_sql = f"""
        DELETE FROM {self.state_table}
        WHERE run_timestamp < date_sub(current_timestamp(), {days_to_keep})
        """

        self.spark.sql(cleanup_sql)

    def get_run_by_id(self, run_id: str) -> dict[str, Any] | None:
        """Get details of a specific run"""

        runs = self.spark.table(self.state_table).filter(col("run_id") == run_id).collect()

        if not runs:
            return None

        run = runs[0]
        return {
            "run_id": run["run_id"],
            "timestamp": run["run_timestamp"],
            "mode": run["processing_mode"],
            "files_processed": run["files_processed"],
            "files_succeeded": run["files_succeeded"],
            "files_failed": run["files_failed"],
            "pages_processed": run["total_pages_processed"],
            "duration_seconds": run["processing_duration_seconds"],
            "configuration": json.loads(run["configuration"]),
        }

    def create_run_id(self) -> str:
        """Generate a unique run ID"""
        return str(uuid.uuid4())
