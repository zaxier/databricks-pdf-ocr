# PDF OCR Pipeline Specification

## 1. Overview

### 1.1 Purpose
This specification defines a code repository for processing PDF documents stored locally, uploading them to Databricks volumes and then using OCR (Optical Character Recognition) via Databricks model serving endpoints available in Databricks (e.g. "databricks-claude-4-sonnet").

### 1.2 Key Requirements
- Incremental processing of PDF docs from volumes using autoloader
- Configurable batch processing with max docs per run
- Idempotent design

### 2.2 Components

### 2.2.1 Uploading pdfs to Volume
- **Type**: Batch job
- **Purpose**: Uploads local pdfs in specified dir to volume path. Avoids creating duplicates and skips files already uploaded.
- **Input**: Local dir, and volume path
- **Output**: Volume with PDFs uploaded

#### 2.2.2 Autoloader Ingestion Service
- **Type**: Streaming job
- **Purpose**: Continuously monitor and ingest new PDF files
- **Input**: Databricks volume path
- **Output**: Source Delta table with binary content

#### 2.2.3 OCR Processing Service
- **Type**: Batch job
- **Purpose**: Process PDFs through Databricks model serving endpoints for text extraction
- **Input**: Unprocessed records from source Delta table
- **Output**: Extracted text in target Delta table

#### 2.2.4 State Manager
- **Purpose**: Track processing state and enable idempotent operations
- **Features**: 
  - Incremental processing tracking
  - Reprocessing capabilities
  - Processing status management

#### 2.3 Idempotency
Pipelines must have idempotency. Use simple logic to achieve this. Remember that autoloader ingestion step is already idempotent given the checkpointing built-in to spark streaming. However the step between the autoloader job and the OCR job we need to be able to note which files have already been processed and have already had OCR extractions so we don't reprocess files. 

REMEMBER that the OCR extraction step is EXPENSIVE!

## 3. Data Schemas

### 3.1 Source Delta Table Schema (`pdf_source`)
```python
source_schema = StructType([
    StructField("file_id", StringType(), nullable=False),  # SHA-256 hash of file path
    StructField("file_path", StringType(), nullable=False),
    StructField("file_name", StringType(), nullable=False),
    StructField("file_size", LongType(), nullable=False),
    StructField("file_content", BinaryType(), nullable=False),
    StructField("modification_time", TimestampType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])
```

### 3.2 Target Delta Table Schema (`pdf_ocr_results`)
```python
target_schema = StructType([
    StructField("result_id", StringType(), nullable=False),  # UUID
    StructField("file_id", StringType(), nullable=False),  # Foreign key to source table
    StructField("page_number", IntegerType(), nullable=False),
    StructField("total_pages", IntegerType(), nullable=False),
    StructField("extracted_text", StringType(), nullable=True),
    StructField("extraction_confidence", DoubleType(), nullable=True),
    StructField("processing_timestamp", TimestampType(), nullable=False),
    StructField("processing_duration_ms", LongType(), nullable=False),
    StructField("ocr_model", StringType(), nullable=False),
    StructField("extraction_status", StringType(), nullable=False),  # 'success', 'failed', 'partial'
    StructField("error_message", StringType(), nullable=True)
])
```

### 3.3 Processing State Table Schema (`pdf_processing_state`)
```python
state_schema = StructType([
    StructField("run_id", StringType(), nullable=False),  # UUID for each run
    StructField("run_timestamp", TimestampType(), nullable=False),
    StructField("processing_mode", StringType(), nullable=False),  # 'incremental', 'reprocess_all', 'reprocess_specific'
    StructField("files_processed", IntegerType(), nullable=False),
    StructField("files_succeeded", IntegerType(), nullable=False),
    StructField("files_failed", IntegerType(), nullable=False),
    StructField("total_pages_processed", IntegerType(), nullable=False),
    StructField("processing_duration_seconds", DoubleType(), nullable=False),
    StructField("configuration", StringType(), nullable=False)  # JSON string of run config
])
```

## 4. Configuration Interface
Use dynaconf for configuration. I have a settings.toml file at the repo root and i'll have a .env file for sensitive variables too. Dynaconf will have to load both of these.

Examples of configuration 
```toml
[autoloader]
source_volume_path = "/Volumes/catalog/schema/pdf_documents"
checkpoint_location = "/Volumes/catalog/schema/checkpoints/pdf_ingestion"
source_table_path = "/catalog/schema/pdf_source"

[ocr_processing]
target_table_path = "/catalog/schema/pdf_ocr_results"
state_table_path = "/catalog/schema/pdf_processing_state"
max_docs_per_run = 100
# max_pages_per_pdf omitted to mean 'process all pages'
processing_mode = "incremental"  # Options: incremental, reprocess_all, reprocess_specific
specific_file_ids = []  # Used only in reprocess_specific mode
batch_size = 10
max_retries = 3
retry_delay_seconds = 60

[claude]
claude_max_tokens = 4096
claude_temperature = 0
image_max_edge_pixels = 1568
image_dpi = 200
```

## 5. Development Strategy
- Keep it simple
- I want to be able to run the code in a simple way locally, connecting via Databricks connect using @packages/lightning/spark.py to create the spark session.
- Use uv python manager. 

### 5.1 OCR Using Databricks Model Serving
I found this code snippet online which could be a good way to call Databricks model endpoints, however you would need to change to provide the image.

```python
import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")

response = client.predict(
    endpoint="your-endpoint-name",
    inputs={
        "messages": [
            {"role": "user", "content": "Write a one-sentence bedtime story about a unicorn."}
        ]
    },
)
print(response["choices"][0]["message"]["content"])
```

