# Databricks Claude OCR Script

This script uses Databricks' Claude model serving endpoint to extract text from PDF files using OCR.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set environment variables:
   ```bash
   export DATABRICKS_WORKSPACE_URL="https://your-workspace.databricks.net"
   export DATABRICKS_TOKEN="your-databricks-personal-access-token"
   ```

## Usage

Run the script:
```bash
python databricks_claude_ocr.py
```

The script will:
1. Load the PDF from `data/example.pdf`
2. Convert each page to an image
3. Send each image to Claude for OCR extraction
4. Save the extracted text to `data/extracted_text.txt`

## Features

- Converts PDF pages to images at 200 DPI
- Automatically resizes images to stay within Claude's limits
- Processes pages sequentially with progress updates
- Preserves formatting and table structure where possible

## Configuration

You can modify the following in the script:
- `dpi`: Image resolution for PDF conversion (default: 200)
- `max_edge`: Maximum image dimension (default: 1568 pixels)
- `max_pages`: Limit the number of pages to process