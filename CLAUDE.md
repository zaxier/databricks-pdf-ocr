# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a production-ready Databricks PDF OCR pipeline that processes PDF documents using Claude AI via Databricks Model Serving. The system consists of two main components:

1. **Autoloader Ingestion**: Continuously monitors PDF files in Databricks volumes and ingests them into Delta tables
2. **OCR Processing**: Batch processes ingested PDFs using Claude API for text extraction

## Architecture

The project follows a modular architecture with clear separation of concerns:

- **Config**: Settings and configuration management (`src/databricks_pdf_ocr/config/`)
- **Handlers**: Core processing logic for autoloader and OCR (`src/databricks_pdf_ocr/handlers/`)
- **Jobs**: Entry points for Databricks jobs (`src/databricks_pdf_ocr/jobs/`)
- **Schemas**: Delta table schema definitions (`src/databricks_pdf_ocr/schemas/`)
- **Utils**: Shared utilities for Spark, Claude client, state management (`src/databricks_pdf_ocr/utils/`)
- **Sync**: Local-to-volume PDF synchronization (`src/databricks_pdf_ocr/sync/`)

## Development Commands

### Setup and Installation
```bash
# Install dependencies using uv
uv sync

# Install in development mode
pip install -e .
```

### Testing
```bash
# Run tests with coverage
pytest
# or
pytest tests/ -v --cov=databricks_pdf_ocr --cov-report=term-missing --cov-report=html
```

### Code Quality
```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type checking
mypy src/
```

### Local Development (PDF Sync)
```bash
# Create volume in Databricks
uv run python -m databricks_pdf_ocr.sync create-volume --catalog <catalog> --schema <schema> --volume pdf_documents

# Upload PDFs from local directory
uv run python -m databricks_pdf_ocr.sync upload data --catalog <catalog> --schema <schema> --volume pdf_documents

# List remote PDFs
uv run python -m databricks_pdf_ocr.sync list-remote --catalog <catalog> --schema <schema> --volume pdf_documents
```

### Pipeline Execution
```bash
# Run autoloader (continuous ingestion)
uv run python -m databricks_pdf_ocr.main --catalog <catalog> --schema <schema> autoloader \
  --source-volume-path /Volumes/<catalog>/<schema>/pdf_documents \
  --checkpoint-location /Volumes/<catalog>/<schema>/checkpoints \
  stream

# Run OCR processing
uv run python -m databricks_pdf_ocr.main --catalog <catalog> --schema <schema> ocr \
  --processing-mode incremental \
  --max-docs-per-run 100 \
  --batch-size 10 \
  process

# Check OCR statistics
uv run python -m databricks_pdf_ocr.main --catalog <catalog> --schema <schema> ocr stats

# Reset failed files
uv run python -m databricks_pdf_ocr.main --catalog <catalog> --schema <schema> ocr reset-failed
```

### Databricks Bundle Deployment
```bash
# Validate bundle configuration
databricks bundle validate --target dev

# Deploy to development
databricks bundle deploy --target dev

# Deploy to production
databricks bundle deploy --target prod

# Run specific job
databricks bundle run pdf_ingestion --target dev
databricks bundle run pdf_ocr_processing --target dev
```

## Key Configuration Files

- **`databricks.yml`**: Asset bundle configuration with job definitions, variables, and environments
- **`pyproject.toml`**: Python project configuration with dependencies, build settings, and tool configurations
- **`requirements.txt`**: Production dependencies for Databricks runtime

## Delta Table Schema

The pipeline uses three main Delta tables:

1. **`pdf_source`**: Stores ingested PDF files with metadata and processing status
2. **`pdf_ocr_results`**: Stores extracted text results per page with confidence scores
3. **`pdf_processing_state`**: Tracks processing runs and metrics

All tables use Delta Lake with Change Data Feed enabled and column mapping for schema evolution.

## Processing Modes

- **`incremental`**: Process only new/pending files (default)
- **`reprocess_all`**: Reprocess all files regardless of status
- **`reprocess_specific`**: Reprocess specific files by file ID

## Environment Variables

Required for local development:
- `DATABRICKS_WORKSPACE_URL`: Your Databricks workspace URL
- `DATABRICKS_TOKEN`: Personal access token for authentication

## Important Notes

- The system requires Python 3.12 and uses `uv` for dependency management
- Claude API integration uses Databricks Model Serving endpoints (not direct Anthropic API)
- All file processing is idempotent with proper error handling and retry logic
- The pipeline supports both streaming (autoloader) and batch (OCR) processing modes
- State management ensures consistent processing across runs and failures
- If you want to run python use `uv run python ...` 