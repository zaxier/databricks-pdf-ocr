"""Data schemas for PDF OCR pipeline."""

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


def get_source_schema() -> StructType:
    """Schema for pdf_source table."""
    return StructType(
        [
            StructField("file_id", StringType(), nullable=False),
            StructField("file_path", StringType(), nullable=False),
            StructField("file_name", StringType(), nullable=False),
            StructField("file_size", LongType(), nullable=False),
            StructField("file_content", BinaryType(), nullable=False),
            StructField("modification_time", TimestampType(), nullable=False),
            StructField("ingestion_timestamp", TimestampType(), nullable=False),
        ]
    )


def get_target_schema() -> StructType:
    """Schema for pdf_ocr_results table."""
    return StructType(
        [
            StructField("result_id", StringType(), nullable=False),
            StructField("file_id", StringType(), nullable=False),
            StructField("page_number", IntegerType(), nullable=False),
            StructField("total_pages", IntegerType(), nullable=False),
            StructField("extracted_text", StringType(), nullable=True),
            StructField("extraction_confidence", DoubleType(), nullable=True),
            StructField("processing_timestamp", TimestampType(), nullable=False),
            StructField("processing_duration_ms", LongType(), nullable=False),
            StructField("ocr_model", StringType(), nullable=False),
            StructField("extraction_status", StringType(), nullable=False),
            StructField("error_message", StringType(), nullable=True),
        ]
    )


def get_state_schema() -> StructType:
    """Schema for pdf_processing_state table."""
    return StructType(
        [
            StructField("run_id", StringType(), nullable=False),
            StructField("run_timestamp", TimestampType(), nullable=False),
            StructField("processing_mode", StringType(), nullable=False),
            StructField("files_processed", IntegerType(), nullable=False),
            StructField("files_succeeded", IntegerType(), nullable=False),
            StructField("files_failed", IntegerType(), nullable=False),
            StructField("total_pages_processed", IntegerType(), nullable=False),
            StructField("processing_duration_seconds", DoubleType(), nullable=False),
            StructField("configuration", StringType(), nullable=False),
        ]
    )


def create_source_table_sql(table_path: str) -> str:
    """SQL to create source table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_path} (
        file_id STRING NOT NULL,
        file_path STRING NOT NULL,
        file_name STRING NOT NULL,
        file_size BIGINT NOT NULL,
        file_content BINARY NOT NULL,
        modification_time TIMESTAMP NOT NULL,
        ingestion_timestamp TIMESTAMP NOT NULL,
        content_hash STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name'
    )
    """


def create_target_table_sql(table_path: str) -> str:
    """SQL to create target table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_path} (
        result_id STRING NOT NULL,
        file_id STRING NOT NULL,
        page_number INT NOT NULL,
        total_pages INT NOT NULL,
        extracted_text STRING,
        extraction_confidence DOUBLE,
        processing_timestamp TIMESTAMP NOT NULL,
        processing_duration_ms BIGINT NOT NULL,
        ocr_model STRING NOT NULL,
        extraction_status STRING NOT NULL,
        error_message STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name'
    )
    """


def create_state_table_sql(table_path: str) -> str:
    """SQL to create state table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table_path} (
        run_id STRING NOT NULL,
        run_timestamp TIMESTAMP NOT NULL,
        processing_mode STRING NOT NULL,
        files_processed INT NOT NULL,
        files_succeeded INT NOT NULL,
        files_failed INT NOT NULL,
        total_pages_processed INT NOT NULL,
        processing_duration_seconds DOUBLE NOT NULL,
        configuration STRING NOT NULL
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name'
    )
    """
