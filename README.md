# Databricks PDF OCR Pipeline

## Prerequisites

- Python 3.12+
- A Databricks workspace with Unity Catalog enabled
- A Databricks Volume to store source PDFs
- A running Databricks Model Serving endpoint for a Claude model (e.g., `databricks-claude-3-7-sonnet`)
- Databricks authentication configured via OAuth (run `databricks auth login --host <your-workspace-url>`)

## Installation

1. Clone the repository
2. Install dependencies using [uv](https://github.com/astral-sh/uv):

   ```bash
   # Create virtual environment and install dependencies
   uv sync
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
PDF_OCR_DATABRICKS_CONFIG_PROFILE=DEFAULT
```

### 3. Environment Switching

Use different environments by setting:

```bash
export PDF_OCR_ENV=development  # or testing, production
```

## Usage

### CLI Usage
```bash
# Sync the fixtures dir to your settings.toml configured volume
uv run sync --create-volume 

# Run full pipeline: Autoload pdfs into delta table and run ocr
uv run pipeline

# Run pipeline in manual stages
uv run autoload 
uv run ocr

# Get recent run history
uv run status
```

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
