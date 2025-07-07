# PDF OCR Pipeline Specification

## 1. Overview

### 1.1 Purpose
This specification defines a production-ready Databricks job for processing PDF documents stored in Databricks volumes using OCR (Optical Character Recognition) via Claude AI. The solution leverages Databricks Asset Bundles for deployment and management.

### 1.2 Key Requirements
- Incremental processing of PDF documents from Databricks volumes
- Scalable architecture using Databricks Autoloader and Delta tables
- Configurable batch processing with max documents per run
- Idempotent design with state management
- Support for reprocessing strategies
- Production-ready with proper error handling and monitoring

## 2. Architecture

### 2.1 High-Level Design
```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  PDF Volume     │────▶│  Autoloader      │────▶│ Source Delta    │
│  Storage        │     │  Ingestion       │     │ Table           │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                           │
                        ┌──────────────────┐               │
                        │  Claude API      │◀──────────────┤
                        │  (OCR Service)   │               │
                        └────────┬─────────┘               │
                                 │                         │
                        ┌────────▼─────────┐     ┌─────────▼────────┐
                        │  Target Delta    │◀────│  OCR Processor   │
                        │  Table           │     │  (Batch Job)     │
                        └──────────────────┘     └──────────────────┘
```

### 2.2 Components

#### 2.2.1 Autoloader Ingestion Service
- **Type**: Streaming job
- **Purpose**: Continuously monitor and ingest new PDF files
- **Input**: Databricks volume path
- **Output**: Source Delta table with binary content

#### 2.2.2 OCR Processing Service
- **Type**: Batch job
- **Purpose**: Process PDFs through Claude API for text extraction
- **Input**: Unprocessed records from source Delta table
- **Output**: Extracted text in target Delta table

#### 2.2.3 State Manager
- **Purpose**: Track processing state and enable idempotent operations
- **Features**: 
  - Incremental processing tracking
  - Reprocessing capabilities
  - Processing status management

### 2.3 Directory Structure
```
    databricks-pdf-ocr/
    ├── databricks.yml          # Asset Bundle 
    configuration
    ├── pyproject.toml          # Python project 
    config with dependencies
    ├── requirements.txt        # Dependencies for 
    production
    ├── README.md              # Project documentation
    ├── resources/             # YAML resource 
    definitions
    │   └── pdf_ocr_config.yml # Additional job 
    configurations
    ├── src/
    │   └── databricks_pdf_ocr/
    │       ├── __init__.py
    │       ├── __main__.py
    │       ├── main.py        # Entry point
    │       ├── config/
    │       │   ├── __init__.py
    │       │   └── settings.py # Configuration 
    classes
    │       ├── handlers/
    │       │   ├── __init__.py
    │       │   ├── base.py    # Abstract base handler
    │       │   ├── autoloader.py # PDF ingestion 
    handler
    │       │   └── ocr_processor.py # OCR extraction 
    handler
    │       ├── utils/
    │       │   ├── __init__.py
    │       │   ├── state_manager.py # State tracking
    │       │   └── claude_client.py # Claude API 
    wrapper
    │       └── schemas/
    │           ├── __init__.py
    │           └── tables.py  # Delta table schemas
    └── tests/
```

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
    StructField("processing_status", StringType(), nullable=False),  # 'pending', 'processing', 'completed', 'failed'
    StructField("processing_attempts", IntegerType(), nullable=False, defaultValue=0),
    StructField("last_error", StringType(), nullable=True)
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
    StructField("claude_model", StringType(), nullable=False),
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

### 4.1 Environment Variables
```yaml
# Required
DATABRICKS_HOST: Databricks workspace URL
DATABRICKS_TOKEN: Authentication token (or use DEFAULT auth)

# Optional
CLAUDE_ENDPOINT_NAME: Name of Claude serving endpoint (default: databricks-claude-3-7-sonnet)
```

### 4.2 Job Parameters
```yaml
# Autoloader Configuration
source_volume_path: /Volumes/catalog/schema/pdf_documents
checkpoint_location: /Volumes/catalog/schema/checkpoints/pdf_ingestion
source_table_path: /catalog/schema/pdf_source

# OCR Processing Configuration
target_table_path: /catalog/schema/pdf_ocr_results
state_table_path: /catalog/schema/pdf_processing_state
max_docs_per_run: 100
max_pages_per_pdf: null  # null means process all pages
processing_mode: incremental  # Options: incremental, reprocess_all, reprocess_specific
specific_file_ids: []  # Used only in reprocess_specific mode
batch_size: 10  # Number of PDFs to process in parallel
max_retries: 3
retry_delay_seconds: 60

# Claude Configuration
claude_max_tokens: 4096
claude_temperature: 0
image_max_edge_pixels: 1568
image_dpi: 200
```

## 5. API Interfaces

### 5.1 Handler Base Class
```python
class PDFHandler(ABC):
    """Abstract base class for PDF processing handlers"""
    
    @abstractmethod
    def process(self, spark: SparkSession, config: ProcessingConfig) -> ProcessingResult:
        """Process PDFs according to handler logic"""
        pass
```

### 5.2 Autoloader Handler Interface
```python
class AutoloaderHandler(PDFHandler):
    def start_stream(self, spark: SparkSession, config: AutoloaderConfig) -> StreamingQuery:
        """Start the autoloader streaming job"""
        
    def stop_stream(self, query: StreamingQuery) -> None:
        """Gracefully stop the streaming job"""
```

### 5.3 OCR Processor Interface
```python
class OCRProcessor(PDFHandler):
    def process_batch(self, spark: SparkSession, config: OCRConfig) -> BatchResult:
        """Process a batch of PDFs"""
        
    def extract_text_from_pdf(self, pdf_content: bytes, file_id: str) -> List[PageResult]:
        """Extract text from a single PDF"""
```

### 5.4 State Manager Interface
```python
class StateManager:
    def get_pending_files(self, mode: str, max_files: int, specific_ids: List[str] = None) -> DataFrame:
        """Get files pending processing based on mode"""
        
    def update_file_status(self, file_id: str, status: str, error: str = None) -> None:
        """Update processing status for a file"""
        
    def record_run_metrics(self, metrics: RunMetrics) -> None:
        """Record metrics for a processing run"""
```

## 6. Processing Logic

### 6.1 Autoloader Logic
1. Monitor configured volume path for new PDF files
2. For each new file:
   - Calculate SHA-256 hash of file path as file_id
   - Read binary content
   - Insert into source Delta table with 'pending' status
   - Commit transaction

### 6.2 OCR Processing Logic
1. Determine files to process based on mode:
   - **Incremental**: Select files with status='pending' or (status='failed' AND attempts < max_retries)
   - **Reprocess All**: Select all files regardless of status
   - **Reprocess Specific**: Select files matching specific_file_ids
2. Limit selection to max_docs_per_run
3. For each PDF (process in batches):
   - Update status to 'processing'
   - Convert PDF to images (one per page)
   - For each page:
     - Resize image if needed
     - Call Claude API for OCR
     - Store result with page metadata
   - Update source table status based on results
   - Record processing metrics

### 6.3 Error Handling
- **Transient Errors**: Retry with exponential backoff
- **Permanent Errors**: Mark as failed after max retries
- **Partial Success**: Store successful pages, mark file as 'partial'
- **API Limits**: Implement rate limiting and queuing

## 7. Deployment Configuration

### 7.1 Databricks Asset Bundle Structure
```yaml
bundle:
  name: databricks-pdf-ocr

variables:
  catalog: ${var.catalog}
  schema: ${var.schema}
  source_volume_path: /Volumes/${var.catalog}/${var.schema}/pdf_documents
  max_docs_per_run: 100
  processing_mode: incremental

resources:
  jobs:
    pdf_ingestion:
      name: PDF Ingestion - Autoloader
      tasks:
        - task_key: ingest_pdfs
          spark_python_task:
            python_file: src/databricks_pdf_ocr/jobs/run_autoloader.py
            parameters: ["--config", "autoloader"]
          cluster_spec:
            spark_version: "13.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2
      continuous:
        pause_status: UNPAUSED

    pdf_ocr_processing:
      name: PDF OCR Processing
      tasks:
        - task_key: process_ocr
          python_wheel_task:
            package_name: databricks_pdf_ocr
            entry_point: process_ocr
            parameters: 
              - "--mode"
              - "${var.processing_mode}"
              - "--max-docs"
              - "${var.max_docs_per_run}"
      schedule:
        quartz_cron_expression: "0 0 */2 * * ?"  # Every 2 hours
        timezone_id: "UTC"
      email_notifications:
        on_failure:
          - ${var.alert_email}
```

## 8. Monitoring and Observability

### 8.1 Metrics to Track
- Files ingested per hour
- OCR processing rate (pages/minute)
- Success/failure rates
- API usage and costs
- Processing latency percentiles

### 8.2 Alerts
- Ingestion pipeline failures
- OCR processing failures > threshold
- API rate limit approaching
- Processing backlog growing

### 8.3 Dashboards
- Real-time ingestion status
- Processing queue depth
- Historical trends
- Error analysis

## 9. Security Considerations

### 9.1 Access Control
- Use Databricks Unity Catalog for table permissions
- Service principal for production deployments
- Secure credential management for API keys

### 9.2 Data Protection
- Encrypt data at rest (Delta tables)
- Audit logging for all operations
- PII detection and handling (if applicable)

## 10. Testing Strategy

### 10.1 Unit Tests
- PDF to image conversion
- State management logic
- Error handling scenarios

### 10.2 Integration Tests
- End-to-end pipeline flow
- Claude API integration
- Delta table operations

### 10.3 Performance Tests
- Load testing with various PDF sizes
- Concurrent processing limits
- Memory usage optimization

## 11. Future Enhancements

### 11.1 Potential Improvements
- Support for additional file formats (TIFF, JPEG)
- Multi-language OCR optimization
- Confidence score thresholds
- Automated quality checks
- Integration with downstream NLP pipelines

### 11.2 Scalability Considerations
- Horizontal scaling of OCR workers
- Caching of processed pages
- Incremental model improvements
- Cost optimization strategies