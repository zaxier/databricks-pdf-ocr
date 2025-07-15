# pdf-parse-claude

OCR extraction from PDFs using Databricks Claude endpoint.

## Installation

### Using uv (recommended)

Install from source:
```bash
git clone https://github.com/zaxier/pdf-parse-claude.git
cd pdf-parse-claude
uv sync
```

Run via cli:

## Configuration

Create a `.env` file in your project root with your Databricks credentials:
```bash
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_ACCESS_TOKEN=your-databricks-personal-access-token
DATABRICKS_ENDPOINT_NAME=databricks-claude-3-7-sonnet
```

## Usage

### Command Line Interface

Extract text from a PDF:
```bash
pdf-parse-claude path/to/your/file.pdf
```

Save output to a file:
```bash
pdf-parse-claude path/to/your/file.pdf -o output.txt
```

Limit number of pages:
```bash
pdf-parse-claude path/to/your/file.pdf --page-limit 10
```

### Batch Processing

For processing multiple PDFs from a directory, use the batch processing script:

```bash
python examples/batch_processing.py
```

By default, this processes all PDFs in the `./data` directory and saves results to `./output`. To use different directories, modify the paths in the script:

```python
results = batch_process_pdfs(
    input_dir="./your-pdf-directory",
    output_dir="./your-output-directory",
    max_workers=3
)
```

This will:
- Process all PDF files in the specified directory
- Use parallel processing (3 workers by default)
- Save extracted text to `{filename}_extracted.txt` files
- Provide progress updates and error handling
- Generate a summary report

### Python API

```python
from pdf_parse_claude import DatabricksClaudeOCR

ocr = DatabricksClaudeOCR(
    workspace_url="https://your-workspace.databricks.com",
    token="your-databricks-personal-access-token",
    endpoint_name="databricks-claude-3-7-sonnet"
)

extracted_text = ocr.extract_text_from_pdf("path/to/your/file.pdf")
print(extracted_text)
```

## Features

- Converts PDF pages to images at 200 DPI
- Automatically resizes images to stay within Claude's limits
- Processes pages sequentially with progress updates
- Preserves formatting and table structure where possible
- Command-line interface for easy integration
- Python API for programmatic use

## Development

### Using uv (recommended)

Install development dependencies:
```bash
uv sync --dev
```

Run tests:
```bash
uv run pytest
```

Run linting:
```bash
uv run black .
uv run ruff check .
uv run mypy .
```

### Using pip

Install development dependencies:
```bash
pip install -e .[dev]
```

Run tests:
```bash
pytest
```

Run linting:
```bash
black .
ruff check .
mypy .