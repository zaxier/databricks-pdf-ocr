"""Autoloader handler for PDF ingestion."""

import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, regexp_extract, sha2
from pyspark.sql.streaming.query import StreamingQuery

from ..config import AutoloaderConfig

logger = logging.getLogger(__name__)


class AutoloaderHandler:
    """Handles PDF ingestion using Databricks Autoloader."""

    def __init__(self, spark: SparkSession, config: AutoloaderConfig):
        self.spark = spark
        self.config = config
        self.workspace_client = WorkspaceClient()

    def ensure_checkpoint_volume_exists(self) -> None:
        """Create the checkpoints volume if it doesn't already exist."""
        volume_info = self.config.checkpoint_volume_info
        catalog = volume_info["catalog"]
        schema = volume_info["schema"]
        volume = volume_info["volume"]

        try:
            # Ensure schema exists
            try:
                self.workspace_client.schemas.get(f"{catalog}.{schema}")
                logger.info(f"Schema '{catalog}.{schema}' already exists.")
            except Exception:
                logger.info(f"Schema '{catalog}.{schema}' not found, creating it.")
                self.workspace_client.schemas.create(name=schema, catalog_name=catalog)
                print(f"Schema '{catalog}.{schema}' created.")

            # Check for volume
            try:
                self.workspace_client.volumes.read(f"{catalog}.{schema}.{volume}")
                logger.info(f"Checkpoints volume '{volume}' already exists.")
            except Exception:
                logger.info(f"Checkpoints volume '{volume}' not found, creating it.")
                self.workspace_client.volumes.create(
                    catalog_name=catalog,
                    schema_name=schema,
                    name=volume,
                    volume_type=VolumeType.MANAGED,
                )
                print(f"Checkpoints volume '{volume}' created successfully.")

        except Exception as e:
            logger.error(f"Failed to ensure checkpoints volume exists: {e}")
            raise

    def setup_autoloader_stream(self) -> DataFrame:
        """Setup autoloader stream for PDF files."""
        autoloader_options = {
            "cloudFiles.format": "binaryFile",
            "cloudFiles.includeExistingFiles": "true",
            "cloudFiles.maxFilesPerTrigger": str(self.config.max_files_per_trigger),
            "cloudFiles.schemaHints": "path STRING, modificationTime TIMESTAMP, length LONG, content BINARY",
        }

        df = (
            self.spark.readStream.format("cloudFiles")
            .options(**autoloader_options)
            .load(self.config.source_volume_path)
        )

        # Filter for PDF files only
        pdf_df = df.filter(col("path").rlike(r".*\.(pdf|PDF)$"))

        # Transform to target schema
        from pyspark.sql.functions import current_timestamp

        transformed_df = pdf_df.select(
            sha2(col("path"), 256).alias("file_id"),
            col("path").alias("file_path"),
            regexp_extract(col("path"), r"([^/]+)$", 1).alias("file_name"),
            col("length").alias("file_size"),
            col("content").alias("file_content"),
            col("modificationTime").alias("modification_time"),
            current_timestamp().alias("ingestion_timestamp"),
            sha2(col("content"), 256).alias("content_hash"),
        )

        return transformed_df

    def start_ingestion_stream(self) -> StreamingQuery:
        """Start the PDF ingestion stream."""
        print(f"Starting PDF ingestion from: {self.config.source_volume_path}")

        # Ensure checkpoints volume exists before starting stream
        print("Ensuring checkpoints volume exists...")
        self.ensure_checkpoint_volume_exists()

        transformed_df = self.setup_autoloader_stream()

        query = (
            transformed_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.config.checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.config.source_table_path)
        )

        return query

    def ingest_pdfs_batch(self) -> None:
        """Run PDF ingestion as a batch job and wait for completion."""
        query = self.start_ingestion_stream()
        query.awaitTermination()
        print("PDF ingestion completed")
