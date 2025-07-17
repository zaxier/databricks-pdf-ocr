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

- Python 3.12+
- A Databricks workspace with Unity Catalog enabled.
- A Databricks Volume to store source PDFs.
- A running Databricks Model Serving endpoint for a Claude model (e.g., `databricks-claude-3-7-sonnet`).
- Databricks SDK configured for authentication (e.g., via `databricks configure` or environment variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN`).
- For Asset Bundle deployment: Databricks CLI v0.205.0+ with bundle support.

## Installation

1.  Clone the repository.
2.  Install the required dependencies using [uv](https://github.com/astral-sh/uv). It is recommended to use a virtual environment, which `uv` can create and manage.

    ```bash
    # Create and activate a virtual environment
    uv sync
    source .venv/bin/activate

    # Install dependencies and the project in editable mode
    uv pip install -r requirements.txt
    uv pip install -e .
    ```

## Deployment Options

There are two main ways to deploy and run this pipeline:

### Option 1: Interactive CLI Usage (Local Development)
Use the CLI commands directly for development, testing, and ad-hoc processing.

### Option 2: Databricks Asset Bundle (Production Deployment)
Deploy the pipeline as managed Databricks jobs using Asset Bundles for production use.

## Asset Bundle Deployment

For production deployments, this project includes a comprehensive Databricks Asset Bundle configuration that automates the deployment of jobs, schemas, and volumes.

### Prerequisites for Bundle Deployment

1. **Databricks CLI**: Install the latest Databricks CLI with bundle support:
   ```bash
   pip install databricks-cli
   # Or upgrade if already installed
   pip install --upgrade databricks-cli
   ```

2. **Authentication**: Configure your Databricks CLI:
   ```bash
   databricks configure
   ```

3. **Service Principal** (for production): Set up a service principal for production deployments.

### Bundle Configuration

The project uses serverless compute by default for cost efficiency and easier management. The bundle configuration includes:

- **Serverless Jobs**: All jobs run on serverless compute by default
- **Classic Cluster Support**: Optional classic cluster configurations available
- **Multi-Environment**: Support for dev, staging, and production environments
- **Python Wheel Packaging**: Automatic wheel building and deployment

### Deployment Steps

1. **Build the Python Wheel**:
   ```bash
   uv build
   ```
   This creates a wheel file in the `dist/` directory that will be deployed to Databricks.

2. **Configure Environment Variables** (if needed):
   ```bash
   # For development, you can use environment variables
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-token"
   ```

3. **Update Configuration**:
   Edit `databricks.yml` to customize variables for your environment:
   ```yaml
   variables:
     catalog: "your_catalog"
     schema: "your_schema"
     alert_email: "your-email@company.com"
     model_endpoint_name: "your-claude-endpoint"
   ```

4. **Validate Bundle**:
   ```bash
   databricks bundle validate --target dev
   ```

5. **Deploy to Development**:
   ```bash
   databricks bundle deploy --target dev
   ```

6. **Deploy to Production**:
   ```bash
   databricks bundle deploy --target prod
   ```

### Running Bundle Jobs

After deployment, you can run jobs using the Databricks CLI:

```bash
# Run PDF ingestion (autoloader)
databricks bundle run pdf_ingestion --target dev

# Run OCR processing
databricks bundle run pdf_ocr_processing --target dev

# Run reprocessing of failed files
databricks bundle run pdf_ocr_reprocess_failed --target dev
```

### Environment Configuration

The bundle supports three environments:

- **dev**: Development environment with jobs paused by default
- **staging**: Staging environment for testing
- **prod**: Production environment with service principal authentication

### Compute Options

**Default (Serverless)**: All jobs use serverless compute by default, which provides:
- Cost efficiency
- No cluster management overhead
- Automatic scaling

**Classic Clusters**: If you need classic clusters for specific workloads:
1. In each job definition in `databricks.yml`, comment out the `environment_key: Default` line
2. Uncomment the `new_cluster` configuration block
3. Optionally, use the example classic cluster jobs in `resources/classic_cluster_jobs.yml.example`

### Monitoring and Alerts

The bundle includes:
- Email notifications for job failures
- Configurable alert emails per environment
- Job timeouts and retry logic
- Comprehensive logging and state tracking

## Interactive CLI Usage

For development and testing, you can run the pipeline components directly using the CLI. All commands should be run with `uv run` to ensure proper environment and dependencies.

```bash
# General help
uv run python -m databricks_pdf_ocr.main --help

# Autoloader help
uv run python -m databricks_pdf_ocr.main autoloader --help

# OCR help
uv run python -m databricks_pdf_ocr.main ocr --help
```

### Step 1: Sync Local PDFs to a Volume (Optional)

If your PDFs are on your local machine, you can use the `sync` utility to upload them to your Databricks Volume.

First, ensure the volume exists. You can create it with the `create-volume` command:
```bash
uv run python -m databricks_pdf_ocr.sync create-volume \
    --catalog my_catalog \
    --schema my_schema \
    --volume my_volume
```

Then, upload your files:
```bash
uv run python -m databricks_pdf_ocr.sync upload /path/to/local/pdfs \
    --catalog my_catalog \
    --schema my_schema \
    --volume my_volume
```

### Step 2: Run the Autoloader to Ingest PDFs

Run the autoloader in `stream` mode to ingest the PDFs from the volume into the `pdf_source` table.

```bash
uv run python -m databricks_pdf_ocr.main autoloader stream \
    --catalog my_catalog \
    --schema my_schema \
    --source-volume-path /Volumes/my_catalog/my_schema/my_volume \
    --checkpoint-location /Volumes/my_catalog/my_schema/my_volume/_checkpoints/autoloader
```
This command will start, process available files, and then stop. For continuous ingestion, you would typically run this as a Databricks job.

### Step 3: Run the OCR Processor

Once files are ingested, run the OCR processor to extract text.

```bash
uv run python -m databricks_pdf_ocr.main ocr process \
    --catalog my_catalog \
    --schema my_schema \
    --max-docs-per-run 100 \
    --batch-size 10
```

### Other Useful Commands

**Check ingestion statistics:**
```bash
uv run python -m databricks_pdf_ocr.main autoloader stats --catalog ... --schema ...
```

**Check OCR processing statistics:**
```bash
uv run python -m databricks_pdf_ocr.main ocr stats --catalog ... --schema ...
```

**List failed OCR files:**
```bash
uv run python -m databricks_pdf_ocr.main ocr failed --catalog ... --schema ...
```

**Reset failed files to be re-processed:**
```bash
uv run python -m databricks_pdf_ocr.main ocr reset-failed --catalog ... --schema ...
```

**Test the connection to the Claude endpoint:**
```bash
uv run python -m databricks_pdf_ocr.main ocr test-claude --catalog ... --schema ...
```

## Configuration

All pipeline parameters are controlled via CLI arguments. Key options include:

-   `--catalog`, `--schema`: Specify the Unity Catalog location for all tables.
-   `--processing-mode`: Set the OCR processor to `incremental`, `reprocess_all`, or `reprocess_specific`.
-   `--batch-size`: The number of PDFs to process in parallel during an OCR run.
-   `--model-endpoint-name`: The name of your Claude model serving endpoint.
