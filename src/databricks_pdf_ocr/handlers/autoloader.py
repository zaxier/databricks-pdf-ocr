"""Autoloader handler for PDF ingestion."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, sha2, regexp_extract
from pyspark.sql.streaming import StreamingQuery

from ..config import AutoloaderConfig


class AutoloaderHandler:
    """Handles PDF ingestion using Databricks Autoloader."""
    
    def __init__(self, spark: SparkSession, config: AutoloaderConfig):
        self.spark = spark
        self.config = config
    
    def setup_autoloader_stream(self) -> DataFrame:
        """Setup autoloader stream for PDF files."""
        autoloader_options = {
            "cloudFiles.format": "binaryFile",
            "cloudFiles.includeExistingFiles": "true", 
            "cloudFiles.maxFilesPerTrigger": str(self.config.max_files_per_trigger),
            "cloudFiles.schemaHints": "path STRING, modificationTime TIMESTAMP, length LONG, content BINARY"
        }
        
        df = (
            self.spark.readStream
            .format("cloudFiles")
            .options(**autoloader_options)
            .load(self.config.source_volume_path)
        )
        
        # Filter for PDF files only
        pdf_df = df.filter(col("path").rlike(r".*\.(pdf|PDF)$"))
        
        # Transform to target schema
        transformed_df = pdf_df.select(
            sha2(col("path"), 256).alias("file_id"),
            col("path").alias("file_path"),
            regexp_extract(col("path"), r"([^/]+)$", 1).alias("file_name"),
            col("length").alias("file_size"),
            col("content").alias("file_content"),
            col("modificationTime").alias("modification_time"),
            current_timestamp().alias("ingestion_timestamp")
        )
        
        return transformed_df
    
    def start_ingestion_stream(self) -> StreamingQuery:
        """Start the PDF ingestion stream."""
        print(f"Starting PDF ingestion from: {self.config.source_volume_path}")
        
        transformed_df = self.setup_autoloader_stream()
        
        query = (
            transformed_df.writeStream
            .format("delta")
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