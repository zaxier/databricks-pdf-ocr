from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.streaming.query import StreamingQuery

from ..config.settings import AutoloaderConfig
from ..handlers.base import PDFHandler, ProcessingResult
from ..schemas.tables import create_source_table


class AutoloaderHandler(PDFHandler):
    """Handles PDF ingestion using Databricks Autoloader"""

    def __init__(self, spark: SparkSession, config: AutoloaderConfig):
        super().__init__(spark, config)
        self.config: AutoloaderConfig = config
        self.streaming_query: StreamingQuery | None = None

        # Ensure source table exists
        self._ensure_source_table_exists()

    def _ensure_source_table_exists(self) -> None:
        """Ensure the source table exists"""
        create_source_table(
            self.spark,
            self.config.source_table_name,
            location=None,  # Let Delta manage the location
        )

    def process(self, **kwargs: Any) -> ProcessingResult:
        """Start the autoloader streaming process"""
        run_id = kwargs.get("run_id", "autoloader_" + str(datetime.now().timestamp()))

        start_time = datetime.now()
        result = ProcessingResult(
            run_id=run_id, start_time=start_time, configuration=self._get_config_dict()
        )

        try:
            self.log_processing_start(run_id, source_path=self.config.source_volume_path)

            # Start streaming query
            self.streaming_query = self.start_stream()

            # For streaming, we return immediately
            # The actual processing happens in the background
            result.finalize()

        except Exception as e:
            self.handle_error(e, "starting autoloader stream")
            raise

        return result

    def start_stream(self) -> StreamingQuery:
        """Start the autoloader streaming job"""
        print(f"Starting Autoloader for path: {self.config.source_volume_path}")

        # Configure autoloader options based on best practices
        autoloader_options = {
            "cloudFiles.format": self.config.file_format,
            "cloudFiles.includeExistingFiles": str(self.config.include_existing_files).lower(),
            "cloudFiles.maxFilesPerTrigger": str(self.config.max_files_per_trigger),
            "cloudFiles.useNotifications": str(self.config.use_notifications).lower(),
            "cloudFiles.schemaLocation": self.config.schema_location_path,
            "cloudFiles.backfillInterval": self.config.backfill_interval,
        }

        # Add schema hints for binary file format
        if self.config.file_format == "binaryFile":
            autoloader_options["cloudFiles.validateOptions"] = "false"
            # Schema hints for binary files
            autoloader_options["cloudFiles.schemaHints"] = (
                "path STRING, modificationTime TIMESTAMP, length LONG, content BINARY"
            )

        # Add max file size if configured
        if self.config.max_file_size_bytes:
            autoloader_options["cloudFiles.maxFileSize"] = str(self.config.max_file_size_bytes)

        # Read from volume using autoloader
        df = (
            self.spark.readStream.format("cloudFiles")
            .options(**autoloader_options)
            .load(self.config.source_volume_path)
        )

        # Filter for PDF files only
        df = df.filter(col("path").rlike(r".*\.(pdf|PDF)$"))

        # Transform to match target schema
        transformed_df = self._transform_autoloader_data(df)

        # Write to Delta table with appropriate trigger
        write_stream = (
            transformed_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.config.checkpoint_path)
            .option("mergeSchema", "true")
            # .foreachBatch(self._process_batch)
        )

        # Use availableNow trigger for batch-like processing (recommended for cost optimization)
        # Default to availableNow for cost-efficient batch processing
        query = write_stream.trigger(availableNow=True).start(self.config.source_table_name)

        return query

    def _transform_autoloader_data(self, df: DataFrame) -> DataFrame:
        """Transform autoloader data to match source table schema"""
        return df.select(
            self._generate_file_id(col("path")).alias("file_id"),
            col("path").alias("file_path"),
            self._extract_file_name(col("path")).alias("file_name"),
            col("length").alias("file_size"),
            col("content").alias("file_content"),
            col("modificationTime").alias("modification_time"),
            current_timestamp().alias("ingestion_timestamp"),
            lit("pending").alias("processing_status"),
            lit(0).alias("processing_attempts"),
            lit(None).cast("string").alias("last_error"),
        )

    def _generate_file_id(self, path_col: Any) -> Any:
        """Generate file ID as SHA-256 hash of file path"""
        from pyspark.sql.functions import sha2

        return sha2(path_col, 256)

    def _extract_file_name(self, path_col: Any) -> Any:
        """Extract file name from full path"""
        from pyspark.sql.functions import regexp_extract

        return regexp_extract(path_col, r"([^/]+)$", 1)

    def _process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Process each batch of files"""
        try:
            print(f"Processing batch {batch_id} with {batch_df.count()} files")

            # Check for duplicates before inserting
            existing_files = (
                self.spark.table(self.config.source_table_name).select("file_id").distinct()
            )

            # Only insert new files
            new_files = batch_df.join(existing_files, on="file_id", how="left_anti")

            new_count = new_files.count()
            if new_count > 0:
                print(f"Inserting {new_count} new files")

                # Insert new files
                new_files.write.mode("append").saveAsTable(self.config.source_table_name)

                # Log inserted files
                file_names = [row.file_name for row in new_files.select("file_name").collect()]
                print(f"Inserted files: {file_names}")
            else:
                print("No new files to insert")

        except Exception as e:
            print(f"Error processing batch {batch_id}: {str(e)}")
            raise

    def stop_stream(self) -> None:
        """Gracefully stop the streaming job"""
        if self.streaming_query is not None:
            print("Stopping autoloader stream...")
            self.streaming_query.stop()
            self.streaming_query = None
            print("Autoloader stream stopped")

    def get_stream_status(self) -> dict[str, Any]:
        """Get current stream status"""
        if self.streaming_query is None:
            return {"status": "stopped", "isActive": False}

        return {
            "status": "running" if self.streaming_query.isActive else "stopped",
            "isActive": self.streaming_query.isActive,
            "id": self.streaming_query.id,
            "name": self.streaming_query.name,
            "lastProgress": self.streaming_query.lastProgress,
            "status_details": self.streaming_query.status,
        }

    def wait_for_termination(self, timeout: int | None = None) -> None:
        """Wait for the streaming query to terminate"""
        if self.streaming_query is not None:
            self.streaming_query.awaitTermination(timeout)

    def get_ingestion_stats(self) -> dict[str, Any]:
        """Get ingestion statistics"""
        try:
            source_table = self.spark.table(self.config.source_table_name)

            # Get counts by status
            status_counts = source_table.groupBy("processing_status").count().collect()

            status_dict = {row.processing_status: row.count for row in status_counts}

            # Get total file size
            total_stats = source_table.agg({"file_size": "sum", "file_id": "count"}).collect()[0]

            # Get recent files
            recent_files = (
                source_table.orderBy(col("ingestion_timestamp").desc())
                .limit(10)
                .select("file_name", "file_size", "ingestion_timestamp", "processing_status")
                .collect()
            )

            return {
                "total_files": total_stats["count(file_id)"],
                "total_size_bytes": total_stats["sum(file_size)"] or 0,
                "status_counts": status_dict,
                "recent_files": [
                    {
                        "file_name": row.file_name,
                        "file_size": row.file_size,
                        "ingestion_timestamp": row.ingestion_timestamp,
                        "processing_status": row.processing_status,
                    }
                    for row in recent_files
                ],
            }

        except Exception as e:
            return {"error": str(e)}

    def rescan_volume(self) -> ProcessingResult:
        """Manually rescan the volume for new files (one-time operation)"""
        run_id = f"rescan_{datetime.now().timestamp()}"
        start_time = datetime.now()

        result = ProcessingResult(
            run_id=run_id, start_time=start_time, configuration=self._get_config_dict()
        )

        try:
            print(f"Rescanning volume: {self.config.source_volume_path}")

            # Read all files from volume
            df = self.spark.read.format("binaryFile").load(self.config.source_volume_path)

            # Filter for PDF files
            pdf_files = df.filter(col("path").rlike(r".*\.(pdf|PDF)$"))

            # Transform to match schema
            transformed_df = self._transform_autoloader_data(pdf_files)

            # Get existing files
            existing_files = (
                self.spark.table(self.config.source_table_name).select("file_id").distinct()
            )

            # Find new files
            new_files = transformed_df.join(existing_files, on="file_id", how="left_anti")

            new_count = new_files.count()

            if new_count > 0:
                print(f"Found {new_count} new files")
                new_files.write.mode("append").saveAsTable(self.config.source_table_name)
                result.total_files_processed = new_count
                result.total_files_succeeded = new_count
            else:
                print("No new files found")

        except Exception as e:
            self.handle_error(e, "rescanning volume")
            raise
        finally:
            result.finalize()
            self.log_processing_end(result)

        return result

    def _get_config_dict(self) -> dict[str, Any]:
        """Get configuration as dictionary"""
        return self.config.to_dict()

    def validate_volume_path(self) -> dict[str, Any]:
        """Validate that the volume path exists and is accessible"""
        try:
            # Try to list files in the volume
            files = self.spark.read.format("binaryFile").load(self.config.source_volume_path)
            file_count = files.count()

            # Count PDF files specifically
            pdf_count = files.filter(col("path").rlike(r".*\.(pdf|PDF)$")).count()

            return {
                "valid": True,
                "path": self.config.source_volume_path,
                "total_files": file_count,
                "pdf_files": pdf_count,
                "accessible": True,
            }

        except Exception as e:
            return {
                "valid": False,
                "path": self.config.source_volume_path,
                "error": str(e),
                "accessible": False,
            }

    def cleanup_checkpoint(self) -> None:
        """Clean up checkpoint location"""
        try:
            print(f"Cleaning up checkpoint location: {self.config.checkpoint_path}")

            # Use dbutils to remove checkpoint directory
            if self.spark.sparkContext._jvm is None:
                raise RuntimeError("JVM not available")
            dbutils = self.spark.sparkContext._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()
            dbutils.fs.rm(self.config.checkpoint_path, True)

            print("Checkpoint cleaned up successfully")

        except Exception as e:
            print(f"Error cleaning up checkpoint: {str(e)}")
            raise

    def get_checkpoint_info(self) -> dict[str, Any]:
        """Get information about the checkpoint"""
        try:
            if self.spark.sparkContext._jvm is None:
                raise RuntimeError("JVM not available")
            dbutils = self.spark.sparkContext._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()

            # Check if checkpoint exists
            try:
                files = dbutils.fs.ls(self.config.checkpoint_path)
                exists = True
                file_count = len(files)
            except Exception:
                exists = False
                file_count = 0

            return {"path": self.config.checkpoint_path, "exists": exists, "file_count": file_count}

        except Exception as e:
            return {"path": self.config.checkpoint_path, "error": str(e)}
