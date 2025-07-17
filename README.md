# Databricks PDF OCR Pipeline

This project provides a robust and scalable pipeline for processing PDF documents using Databricks and Claude. It automates the ingestion of PDFs from a Databricks Volume, performs OCR (Optical Character Recognition) using a Claude model serving endpoint, and stores the structured results in Delta tables for further analysis.

## Features

- **Two-Stage Pipeline**: Decoupled ingestion (Autoloader) and processing (OCR) for better scalability and resilience.
- **Scalable Ingestion**: Uses Databricks Autoloader to efficiently ingest new PDFs from a volume.
- **High-Quality OCR**: Leverages state-of-the-art Claude models via Databricks Model Serving for accurate text extraction.
- **State Management**: Utilizes Delta tables to track the processing state of each document, enabling incremental processing and easy error handling.
- **Resilience**: Includes automatic retries for failed documents.
- **Flexible Processing Modes**: Supports incremental processing, reprocessing all documents, or reprocessing specific files by ID.
- **Command-Line Interface**: A comprehensive CLI for running and managing the pipeline.
- **File Sync Utility**: A helper utility to sync local PDF files to a Databricks Volume.

## Architecture

The pipeline consists of three main components:

1.  **PDF Sync (`sync`)**: A utility to upload local PDF files to a specified Databricks Volume. It uses file hashes to avoid uploading duplicates.
2.  **Autoloader (`autoloader`)**: A streaming job that monitors the Databricks Volume for new PDF files. It ingests the binary content of the PDFs and records their metadata into the `pdf_source` Delta table, marking them as `pending`.
3.  **OCR Processor (`ocr`)**: A job that queries the `pdf_source` table for pending files. It processes them in batches, sending each page to the Claude model endpoint for OCR. The extracted text and metadata are saved to the `pdf_ocr_results` table, and the status in the `pdf_source` table is updated to `completed` or `failed`.

All state is managed in three Delta tables:
-   `pdf_source`: Stores the raw PDF data and its current processing status.
-   `pdf_ocr_results`: Stores the extracted text results for each page of a PDF.
-   `pdf_processing_state`: Stores metadata and metrics for each pipeline run.

## Prerequisites

- Python 3.9+
- A Databricks workspace with Unity Catalog enabled.
- A Databricks Volume to store source PDFs.
- A running Databricks Model Serving endpoint for a Claude model (e.g., `databricks-claude-3-7-sonnet`).
- Databricks SDK configured for authentication (e.g., via `databricks configure` or environment variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN`).

## Installation

1.  Clone the repository.
2.  Install the required dependencies. It is recommended to use a virtual environment.

    ```bash
    pip install -r requirements.txt
    ```

3.  Install the project in editable mode to make the CLI available:
    ```bash
    pip install -e .
    ```

## Usage

The primary way to interact with the pipeline is through the main CLI.

```bash
# General help
python -m databricks_pdf_ocr.main --help

# Autoloader help
python -m databricks_pdf_ocr.main autoloader --help

# OCR help
python -m databricks_pdf_ocr.main ocr --help
```

### Step 1: Sync Local PDFs to a Volume (Optional)

If your PDFs are on your local machine, you can use the `sync` utility to upload them to your Databricks Volume.

First, ensure the volume exists. You can create it with the `create-volume` command:
```bash
python -m databricks_pdf_ocr.sync create-volume \
    --catalog my_catalog \
    --schema my_schema \
    --volume my_volume
```

Then, upload your files:
```bash
python -m databricks_pdf_ocr.sync upload /path/to/local/pdfs \
    --catalog my_catalog \
    --schema my_schema \
    --volume my_volume
```

### Step 2: Run the Autoloader to Ingest PDFs

Run the autoloader in `stream` mode to ingest the PDFs from the volume into the `pdf_source` table.

```bash
python -m databricks_pdf_ocr.main autoloader stream \
    --catalog my_catalog \
    --schema my_schema \
    --source-volume-path /Volumes/my_catalog/my_schema/my_volume \
    --checkpoint-location /Volumes/my_catalog/my_schema/my_volume/_checkpoints/autoloader
```
This command will start, process available files, and then stop. For continuous ingestion, you would typically run this as a Databricks job.

### Step 3: Run the OCR Processor

Once files are ingested, run the OCR processor to extract text.

```bash
python -m databricks_pdf_ocr.main ocr process \
    --catalog my_catalog \
    --schema my_schema \
    --max-docs-per-run 100 \
    --batch-size 10
```

### Other Useful Commands

**Check ingestion statistics:**
```bash
python -m databricks_pdf_ocr.main autoloader stats --catalog ... --schema ...
```

**Check OCR processing statistics:**
```bash
python -m databricks_pdf_ocr.main ocr stats --catalog ... --schema ...
```

**List failed OCR files:**
```bash
python -m databricks_pdf_ocr.main ocr failed --catalog ... --schema ...
```

**Reset failed files to be re-processed:**
```bash
python -m databricks_pdf_ocr.main ocr reset-failed --catalog ... --schema ...
```

**Test the connection to the Claude endpoint:**
```bash
python -m databricks_pdf_ocr.main ocr test-claude --catalog ... --schema ...
```

## Configuration

All pipeline parameters are controlled via CLI arguments. Key options include:

-   `--catalog`, `--schema`: Specify the Unity Catalog location for all tables.
-   `--processing-mode`: Set the OCR processor to `incremental`, `reprocess_all`, or `reprocess_specific`.
-   `--batch-size`: The number of PDFs to process in parallel during an OCR run.
-   `--model-endpoint-name`: The name of your Claude model serving endpoint.
