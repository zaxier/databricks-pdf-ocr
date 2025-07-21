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
# Run unit tests only
uv run pytest tests/ -k "not integration"

# Run all tests (including integration tests - requires Databricks environment)
uv run pytest -v                  # Verbose output
uv run pytest --cov               # With coverage

# Run specific test categories
uv run pytest tests/test_sync.py  # Run specific test file
uv run pytest -m integration      # Run only integration tests
uv run pytest tests/integration/  # Run all integration tests

# Integration test environment setup
export PDF_OCR_ENV=testing        # Use testing environment configuration

# Linting and formatting
uv run ruff check .               # Lint code
uv run ruff format .              # Format code (replaces black)

# Type checking
uv run mypy src                   # Type check source code
```

### Integration Testing

The integration tests use real Databricks infrastructure (Spark, volumes, Delta tables) while mocking only the Claude API endpoints to control costs and ensure predictable responses.

#### Prerequisites for Integration Tests
- Active Databricks workspace with databricks-connect configured
- Unity Catalog enabled with appropriate permissions
- Test catalog/schema access (uses `zaxier_dev.pdf_ocr_test.*`)
- Environment variable: `export PDF_OCR_ENV=testing`

#### Running Integration Tests
```bash
# Set testing environment
export PDF_OCR_ENV=testing

# Run all integration tests
uv run pytest tests/integration/ -v

# Run specific integration test modules
uv run pytest tests/integration/test_sync_integration.py   # File sync tests
uv run pytest tests/integration/test_autoloader_integration.py  # Streaming ingestion tests
uv run pytest tests/integration/test_ocr_integration.py    # OCR processing tests

# Run with coverage
uv run pytest tests/integration/ --cov=databricks_pdf_ocr
```

#### Integration Test Coverage
- **Full Pipeline Tests**: Complete workflow from sync → autoload → OCR → state tracking
- **Sync Tests**: File upload to Databricks volumes using volsync library
- **Autoloader Tests**: Real streaming ingestion from volumes to Delta tables
- **OCR Tests**: PDF processing with mocked Claude API responses
- **Error Handling**: Network failures, invalid files, partial batch failures
- **State Management**: Run tracking, incremental processing, history

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
   Local PDFs � Databricks Volume � Autoloader � OCR Processing � Delta Tables
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