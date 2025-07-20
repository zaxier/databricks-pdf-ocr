# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Running the Application
```bash
# Main pipeline command (full PDF OCR processing)
uv run pipeline [--mode incremental|reprocess_all|reprocess_specific]

# Sync local PDFs to Databricks volume
uv run sync [--source-path ./data] [--target-volume pdf_source]

# Individual pipeline steps
uv run autoload    # Run PDF ingestion only
uv run ocr         # Run OCR processing only  
uv run status      # Show processing status/history
```

### Testing, Linting, and Type Checking
```bash
# Run tests
uv run pytest
uv run pytest tests/test_sync.py  # Run specific test file
uv run pytest -v                  # Verbose output
uv run pytest --cov               # With coverage

# Linting and formatting
uv run ruff check .               # Lint code
uv run ruff format .              # Format code (replaces black)

# Type checking
uv run mypy src                   # Type check source code
```

### Building
```bash
uv build                          # Build wheel and source distribution
```

## Architecture Overview

This is a **Databricks PDF OCR Pipeline** that processes PDF documents using Claude AI models. The system is designed for production use with comprehensive error handling, state management, and scalability.

### Core Components

1. **Pipeline Orchestration** (`src/databricks_pdf_ocr/main.py`)
   - Coordinates the full PDF processing workflow
   - Supports three modes: incremental, reprocess_all, reprocess_specific
   - Manages state tracking and error handling

2. **Data Flow**
   ```
   Local PDFs ’ Databricks Volume ’ Autoloader ’ OCR Processing ’ Delta Tables
   ```
   - **Sync** (`sync.py`): Uploads local PDFs to Databricks Volume
   - **Autoloader** (`handlers/autoloader.py`): Ingests PDFs into `pdf_source` table
   - **OCR** (`processors/ocr.py` + `clients/claude.py`): Extracts text via Claude
   - **State Manager** (`managers/state.py`): Tracks processing runs

3. **Data Storage** (Unity Catalog)
   - `pdf_source`: Raw PDF data and metadata
   - `pdf_ocr_results`: Extracted text per page
   - `pdf_processing_state`: Pipeline run metadata

4. **Configuration** (`config.py` + `settings.toml`)
   - Uses Dynaconf for environment-based configuration
   - Pydantic models for type safety
   - Environment switching via `PDF_OCR_ENV`

### Key Design Patterns

- **Idempotent Operations**: Prevents duplicate processing using state tracking
- **Batch Processing**: Configurable max documents per run
- **Error Resilience**: Failed documents don't block the pipeline
- **Modular Architecture**: Clear separation between ingestion, processing, and storage

### Important Considerations

- OCR processing is expensive - monitor costs carefully
- Claude model endpoint must be configured in Databricks Model Serving
- Requires Databricks workspace with Unity Catalog enabled
- Python 3.12 required due to dependency constraints