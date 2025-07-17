from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_source_table_schema() -> StructType:
    """Get the schema for the PDF source table"""
    return StructType(
        [
            StructField("file_id", StringType(), nullable=False),  # SHA-256 hash of file path
            StructField("file_path", StringType(), nullable=False),
            StructField("file_name", StringType(), nullable=False),
            StructField("file_size", LongType(), nullable=False),
            StructField("file_content", BinaryType(), nullable=False),
            StructField("modification_time", TimestampType(), nullable=False),
            StructField("ingestion_timestamp", TimestampType(), nullable=False),
            StructField(
                "processing_status", StringType(), nullable=False
            ),  # 'pending', 'processing', 'completed', 'failed'
            StructField("processing_attempts", IntegerType(), nullable=False),
            StructField("last_error", StringType(), nullable=True),
        ]
    )


def get_target_table_schema() -> StructType:
    """Get the schema for the OCR results table"""
    return StructType(
        [
            StructField("result_id", StringType(), nullable=False),  # UUID
            StructField("file_id", StringType(), nullable=False),  # Foreign key to source table
            StructField("page_number", IntegerType(), nullable=False),
            StructField("total_pages", IntegerType(), nullable=False),
            StructField("extracted_text", StringType(), nullable=True),
            StructField("extraction_confidence", DoubleType(), nullable=True),
            StructField("processing_timestamp", TimestampType(), nullable=False),
            StructField("processing_duration_ms", LongType(), nullable=False),
            StructField("ocr_model", StringType(), nullable=False),
            StructField(
                "extraction_status", StringType(), nullable=False
            ),  # 'success', 'failed', 'partial'
            StructField("error_message", StringType(), nullable=True),
        ]
    )


def get_state_table_schema() -> StructType:
    """Get the schema for the processing state table"""
    return StructType(
        [
            StructField("run_id", StringType(), nullable=False),  # UUID for each run
            StructField("run_timestamp", TimestampType(), nullable=False),
            StructField(
                "processing_mode", StringType(), nullable=False
            ),  # 'incremental', 'reprocess_all', 'reprocess_specific'
            StructField("files_processed", LongType(), nullable=False),
            StructField("files_succeeded", LongType(), nullable=False),
            StructField("files_failed", LongType(), nullable=False),
            StructField("total_pages_processed", LongType(), nullable=False),
            StructField("processing_duration_seconds", DoubleType(), nullable=False),
            StructField("configuration", StringType(), nullable=False),  # JSON string of run config
        ]
    )


def create_source_table(spark: SparkSession, table_name: str, location: str | None = None) -> None:
    """Create the PDF source table if it doesn't exist"""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        file_id STRING NOT NULL COMMENT 'SHA-256 hash of file path',
        file_path STRING NOT NULL COMMENT 'Full path to PDF file in volume',
        file_name STRING NOT NULL COMMENT 'Name of the PDF file',
        file_size BIGINT NOT NULL COMMENT 'Size of file in bytes',
        file_content BINARY NOT NULL COMMENT 'Binary content of the PDF',
        modification_time TIMESTAMP NOT NULL COMMENT 'Last modification time of the file',
        ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'When the file was ingested',
        processing_status STRING NOT NULL COMMENT 'Current processing status: pending, processing, completed, failed',
        processing_attempts INT NOT NULL COMMENT 'Number of processing attempts',
        last_error STRING COMMENT 'Last error message if any'
    )
    USING DELTA
    """

    if location:
        ddl += f" LOCATION '{location}'"

    ddl += """
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
    """

    spark.sql(ddl)

    # Add constraints if they don't exist
    try:
        spark.sql(
            f"""
            ALTER TABLE {table_name} ADD CONSTRAINT file_id_not_null CHECK (file_id IS NOT NULL)
        """
        )
    except Exception as e:
        if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"Constraint 'file_id_not_null' already exists on {table_name}, skipping")
        else:
            raise

    try:
        spark.sql(
            f"""
            ALTER TABLE {table_name} ADD CONSTRAINT valid_status
            CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed'))
        """
        )
    except Exception as e:
        if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"Constraint 'valid_status' already exists on {table_name}, skipping")
        else:
            raise


def create_target_table(spark: SparkSession, table_name: str, location: str | None = None) -> None:
    """Create the OCR results table if it doesn't exist"""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        result_id STRING NOT NULL COMMENT 'Unique ID for this result',
        file_id STRING NOT NULL COMMENT 'Reference to source table file_id',
        page_number INT NOT NULL COMMENT 'Page number within the PDF',
        total_pages INT NOT NULL COMMENT 'Total number of pages in the PDF',
        extracted_text STRING COMMENT 'Extracted text from the page',
        extraction_confidence DOUBLE COMMENT 'Confidence score of extraction',
        processing_timestamp TIMESTAMP NOT NULL COMMENT 'When the extraction occurred',
        processing_duration_ms BIGINT NOT NULL COMMENT 'Time taken for extraction in milliseconds',
        ocr_model STRING NOT NULL COMMENT 'Model used for OCR',
        extraction_status STRING NOT NULL COMMENT 'Status: success, failed, partial',
        error_message STRING COMMENT 'Error details if extraction failed'
    )
    USING DELTA
    """

    if location:
        ddl += f" LOCATION '{location}'"

    ddl += """
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
    """

    spark.sql(ddl)

    # Add constraints if they don't exist
    try:
        spark.sql(
            f"""
            ALTER TABLE {table_name} ADD CONSTRAINT result_id_not_null CHECK (result_id IS NOT NULL)
        """
        )
    except Exception as e:
        if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"Constraint 'result_id_not_null' already exists on {table_name}, skipping")
        else:
            raise

    try:
        spark.sql(
            f"""
            ALTER TABLE {table_name} ADD CONSTRAINT valid_extraction_status
            CHECK (extraction_status IN ('success', 'failed', 'partial'))
        """
        )
    except Exception as e:
        if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"Constraint 'valid_extraction_status' already exists on {table_name}, skipping")
        else:
            raise

    try:
        spark.sql(
            f"""
            ALTER TABLE {table_name} ADD CONSTRAINT page_number_positive CHECK (page_number > 0)
        """
        )
    except Exception as e:
        if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"Constraint 'page_number_positive' already exists on {table_name}, skipping")
        else:
            raise


def create_state_table(spark: SparkSession, table_name: str, location: str | None = None) -> None:
    """Create the processing state table if it doesn't exist"""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        run_id STRING NOT NULL COMMENT 'Unique ID for this processing run',
        run_timestamp TIMESTAMP NOT NULL COMMENT 'When the run started',
        processing_mode STRING NOT NULL COMMENT 'Mode: incremental, reprocess_all, reprocess_specific',
        files_processed BIGINT NOT NULL COMMENT 'Total files processed in this run',
        files_succeeded BIGINT NOT NULL COMMENT 'Files successfully processed',
        files_failed BIGINT NOT NULL COMMENT 'Files that failed processing',
        total_pages_processed BIGINT NOT NULL COMMENT 'Total pages processed across all files',
        processing_duration_seconds DOUBLE NOT NULL COMMENT 'Total processing time in seconds',
        configuration STRING NOT NULL COMMENT 'JSON configuration used for this run'
    )
    USING DELTA
    """

    if location:
        ddl += f" LOCATION '{location}'"

    ddl += """
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
    """

    spark.sql(ddl)

    # Add constraints if they don't exist
    try:
        spark.sql(
            f"""
            ALTER TABLE {table_name} ADD CONSTRAINT run_id_not_null CHECK (run_id IS NOT NULL)
        """
        )
    except Exception as e:
        if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"Constraint 'run_id_not_null' already exists on {table_name}, skipping")
        else:
            raise

    try:
        spark.sql(
            f"""
            ALTER TABLE {table_name} ADD CONSTRAINT valid_processing_mode
            CHECK (processing_mode IN ('incremental', 'reprocess_all', 'reprocess_specific'))
        """
        )
    except Exception as e:
        if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"Constraint 'valid_processing_mode' already exists on {table_name}, skipping")
        else:
            raise

    try:
        spark.sql(
            f"""
            ALTER TABLE {table_name} ADD CONSTRAINT non_negative_counts
            CHECK (files_processed >= 0 AND files_succeeded >= 0 AND files_failed >= 0)
        """
        )
    except Exception as e:
        if "DELTA_CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"Constraint 'non_negative_counts' already exists on {table_name}, skipping")
        else:
            raise


def ensure_all_tables_exist(spark: SparkSession, catalog: str, schema: str) -> None:
    """Ensure all required tables exist in the specified catalog and schema"""
    # Ensure catalog and schema exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Create tables
    create_source_table(spark, f"{catalog}.{schema}.pdf_source")
    create_target_table(spark, f"{catalog}.{schema}.pdf_ocr_results")
    create_state_table(spark, f"{catalog}.{schema}.pdf_processing_state")
