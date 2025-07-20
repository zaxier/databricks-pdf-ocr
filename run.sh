#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# ==============================================================================
# Configuration
# ==============================================================================
# Easily configure the parameters for the PDF OCR pipeline.

# Databricks Unity Catalog details
CATALOG="zaxier_dev"
SCHEMA="dev_pdf_ocr"
VOLUME="pdf_documents"

# OCR process settings
PROCESSING_MODE="incremental" # "incremental" or "full"
MAX_DOCS_PER_RUN="100"
MAX_PAGES_PER_PDF="10"
BATCH_SIZE="10"

# ==============================================================================
# Do not edit below this line unless you know what you are doing.
# ==============================================================================

# Derived paths
SOURCE_VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}"
CHECKPOINT_LOCATION="/Volumes/${CATALOG}/${SCHEMA}/checkpoints"

# --- Script ---

echo "Step 1: Creating pdf_documents volume..."
uv run python -m databricks_pdf_ocr.sync create-volume \
  --catalog "${CATALOG}" \
  --schema "${SCHEMA}" \
  --volume "${VOLUME}"
echo "Volume creation command finished."
echo

echo "Step 2: Creating checkpoints volume..."
uv run python -m databricks_pdf_ocr.sync create-volume \
  --catalog "${CATALOG}" \
  --schema "${SCHEMA}" \
  --volume "checkpoints"
echo "Volume creation command finished."
echo

echo "Step 3: Uploading data..."
uv run python -m databricks_pdf_ocr.sync upload data \
  --catalog "${CATALOG}" \
  --schema "${SCHEMA}" \
  --volume "${VOLUME}"
echo "Data upload command finished."
echo

echo "Step 4: Running autoloader..."
uv run python -m databricks_pdf_ocr.main \
  --catalog "${CATALOG}" \
  --schema "${SCHEMA}" \
  autoloader \
  --source-volume-path "${SOURCE_VOLUME_PATH}" \
  --checkpoint-location "${CHECKPOINT_LOCATION}" \
  stream
echo "Autoloader command finished."
echo

echo "Step 5: Running OCR..."
uv run python -m databricks_pdf_ocr.main \
  --catalog "${CATALOG}" \
  --schema "${SCHEMA}" \
  ocr \
  --processing-mode "${PROCESSING_MODE}" \
  --max-docs-per-run "${MAX_DOCS_PER_RUN}" \
  --max-pages-per-pdf "${MAX_PAGES_PER_PDF}" \
  --batch-size "${BATCH_SIZE}" \
  process
echo "OCR command finished."
echo

echo "All steps completed successfully."
