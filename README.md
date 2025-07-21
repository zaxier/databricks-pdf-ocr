# Databricks PDF OCR Pipeline

This project provides a robust and scalable pipeline for processing PDF documents using Databricks and Claude. It automates the ingestion of PDFs from a Databricks Volume, performs OCR (Optical Character Recognition) using a Claude model serving endpoint, and stores the structured results in Delta tables for further analysis.

## Features

- **Modular Architecture**: Clean separation of concerns with handlers, clients, processors, and managers
- **Configuration Management**: Uses dynaconf for flexible environment-based configuration
- **Scalable Ingestion**: Uses Databricks Autoloader to efficiently ingest new PDFs from a volume
- **High-Quality OCR**: Leverages Claude models via Databricks Model Serving for accurate text extraction
- **State Management**: Tracks processing state and enables idempotent operations
- **Flexible Processing Modes**: Supports incremental processing, reprocessing all documents, or reprocessing specific files by ID
- **Python Package**: Clean API for programmatic usage and integration

## Architecture

The pipeline consists of a modular Python package with the following components:

1. **AutoloaderHandler**: Manages PDF ingestion from Databricks Volumes using Autoloader
2. **ClaudeClient**: Handles OCR processing via Databricks Model Serving endpoints
3. **OCRProcessor**: Orchestrates PDF-to-image conversion and text extraction
4. **StateManager**: Tracks processing runs and maintains idempotent operations
5. **PDFOCRPipeline**: Main orchestrator that ties all components together

## Data Storage

All state is managed in three Delta tables:
- `pdf_source`: Stores the raw PDF data and metadata
- `pdf_ocr_results`: Stores the extracted text results for each page of a PDF
- `pdf_processing_state`: Stores metadata and metrics for each pipeline run

## Prerequisites

- Python 3.11+
- A Databricks workspace with Unity Catalog enabled
- A Databricks Volume to store source PDFs
- A running Databricks Model Serving endpoint for a Claude model (e.g., `databricks-claude-3-7-sonnet`)
- Databricks authentication configured (environment variables `DATABRICKS_HOST` and `DATABRICKS_ACCESS_TOKEN`)

## Installation

1. Clone the repository
2. Install dependencies using [uv](https://github.com/astral-sh/uv):

   ```bash
   # Create virtual environment and install dependencies
   uv sync
   
   # Activate the virtual environment
   source .venv/bin/activate
   
   # Install the package in editable mode
   uv pip install -e .
   ```

## Configuration

The pipeline uses dynaconf for configuration management. Configuration is stored in `settings.toml` with environment-specific overrides and sensitive values in `.env`.

### 1. Configure settings.toml

Update the `settings.toml` file with your Databricks workspace details:

```toml
[autoloader]
source_volume_path = "/Volumes/your_catalog/your_schema/pdf_documents"
checkpoint_location = "/Volumes/your_catalog/your_schema/checkpoints/pdf_ingestion"
source_table_path = "your_catalog.your_schema.pdf_source"

[ocr_processing]
target_table_path = "your_catalog.your_schema.pdf_ocr_results"
state_table_path = "your_catalog.your_schema.pdf_processing_state"
max_docs_per_run = 100

[claude]
endpoint_name = "databricks-claude-3-7-sonnet"
```

### 2. Set Environment Variables

Create a `.env` file in the project root:

```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_ACCESS_TOKEN=your-access-token
```

### 3. Environment Switching

Use different environments by setting:

```bash
export PDF_OCR_ENV=development  # or production
```

## Usage

### Python API Usage

The primary way to use the pipeline is through the Python API:

```python
from databricks_pdf_ocr import create_pipeline

# Create pipeline instance
pipeline = create_pipeline()

# Setup required tables
pipeline.setup_tables()

# Run full pipeline (ingestion + OCR processing)
result = pipeline.run_full_pipeline()
print(f"Processing completed: {result}")

# Or run components separately
pipeline.run_ingestion()
result = pipeline.run_ocr_processing()

# Check processing history
history = pipeline.get_processing_history()
print(f"Recent runs: {history}")
```

### Local Development with Databricks Connect

For local development, ensure you have Databricks Connect configured to connect to your workspace. The pipeline will automatically use the spark session from `packages.lightning.spark` if available, or create a basic SparkSession.

### Running as a Script

You can also run the pipeline directly:

```bash
# Run the full pipeline
python -m databricks_pdf_ocr.main

# Or import and run in a notebook/script
from databricks_pdf_ocr import create_pipeline
pipeline = create_pipeline()
pipeline.run_full_pipeline()
```

### Processing Modes

The pipeline supports three processing modes:

1. **Incremental** (default): Only processes new files not yet in the results table
2. **Reprocess All**: Reprocesses all files in the source table
3. **Reprocess Specific**: Reprocesses only specified file IDs

Configure the mode in `settings.toml`:

```toml
[ocr_processing]
processing_mode = "incremental"  # or "reprocess_all" or "reprocess_specific"
specific_file_ids = ["file_id_1", "file_id_2"]  # for reprocess_specific mode
```

### Error Handling and Monitoring

The pipeline includes comprehensive error handling and state tracking:

- Each processing run is tracked in the `pdf_processing_state` table
- Failed files are marked with error messages
- Processing statistics are maintained for monitoring
- Idempotent operations prevent duplicate processing

## Development

### Project Structure

```
src/databricks_pdf_ocr/
├── __init__.py              # Package exports
├── config.py                # Configuration management
├── schemas.py               # PySpark schema definitions
├── main.py                  # Main pipeline orchestration
├── handlers/
│   └── autoloader.py        # PDF ingestion via autoloader
├── clients/
│   └── claude.py           # Claude API client for OCR
├── processors/
│   └── ocr.py              # OCR processing logic
└── managers/
    └── state.py             # Processing state management
```

### Running Tests

```bash
# Run tests with pytest
uv run pytest

# Run with coverage
uv run pytest --cov=databricks_pdf_ocr
```
