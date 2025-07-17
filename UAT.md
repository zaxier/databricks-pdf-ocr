# User Acceptance Testing (UAT)

This document outlines the User Acceptance Testing (UAT) procedures for the Databricks PDF OCR Pipeline. These tests are designed to verify that the core functionalities of the repository are working as expected in a real-world environment.

## Scenario 1: End-to-End Happy Path

This scenario tests the entire pipeline from PDF ingestion to successful OCR processing and extraction.

**Prerequisites:**

*   A Databricks workspace.
*   A Databricks cluster with the required libraries installed.
*   A running Databricks Model Serving endpoint for Claude (e.g., `databricks-claude-4-sonnet`) that your user or service principal has permission to query.
*   A PDF file (e.g., `data/example.pdf`) in your local filesystem.

**Steps:**

1.  **Create a Databricks Volume:**
    *   This command creates a new volume in your Databricks catalog to store the PDFs.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.sync create-volume \
          --catalog <your_catalog> \
          --schema <your_schema> \
          --volume pdf_documents
        ```

2.  **Upload a PDF:**
    *   This command uploads the `example.pdf` file to the newly created volume.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.sync upload data \
          --catalog <your_catalog> \
          --schema <your_schema> \
          --volume pdf_documents
        ```

3.  **Run the Autoloader in Streaming Mode:**
    *   This command starts a streaming job that watches the `pdf_documents` volume for new PDFs and ingests them into a Delta table.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.main \
          --catalog <your_catalog> \
          --schema <your_schema> \
          autoloader \
          --source-volume-path /Volumes/<your_catalog>/<your_schema>/pdf_documents \
          --checkpoint-location /Volumes/<your_catalog>/<your_schema>/checkpoints \
          stream
        ```
    *   **Expected Outcome:** The command should start a streaming query. You should see output indicating that the stream is running. After a few moments, you can stop the stream with `Ctrl+C`.

4.  **Verify Ingestion:**
    *   Go to your Databricks workspace and navigate to the catalog and schema you specified.
    *   You should see a new table named `pdf_source`.
    *   Query the `pdf_source` table. It should contain one row for the `example.pdf` file you uploaded.

5.  **Run the OCR Process:**
    *   This command processes the newly ingested PDF, extracts the text using Claude, and stores the results in a new Delta table.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.main \
          --catalog <your_catalog> \
          --schema <your_schema> \
          ocr \
          --processing-mode incremental \
          process
        ```
    *   **Expected Outcome:** The command should run and complete successfully. You should see output indicating that one file was processed successfully.

6.  **Verify OCR Results:**
    *   In your Databricks workspace, you should see two new tables: `pdf_ocr_results` and `pdf_processing_state`.
    *   Query the `pdf_ocr_results` table. It should contain the extracted text from the `example.pdf` file.
    *   Query the `pdf_processing_state` table. It should show that the `example.pdf` file was processed successfully.

## Scenario 2: Handling Failed OCR

This scenario tests the pipeline's ability to handle a failed OCR attempt and then successfully reprocess the failed document.

**Prerequisites:**

*   Scenario 1 has been completed.

**Steps:**

1.  **Upload a New PDF:**
    *   Upload a second PDF (e.g., `data/example2.pdf`) to the `pdf_documents` volume using the `sync upload` command from Scenario 1.

2.  **Run the Autoloader:**
    *   Run the autoloader again to ingest the new PDF. You can use the `rescan` command for a one-time batch ingestion.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.main \
          --catalog <your_catalog> \
          --schema <your_schema> \
          autoloader \
          --source-volume-path /Volumes/<your_catalog>/<your_schema>/pdf_documents \
          --checkpoint-location /Volumes/<your_catalog>/<your_schema>/checkpoints \
          rescan
        ```

3.  **Run OCR with an Invalid Endpoint (and expect failure):**
    *   Run the OCR process, but override the model endpoint with a name that does not exist. This will simulate a configuration error.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.main \
          --catalog <your_catalog> \
          --schema <your_schema> \
          ocr \
          --processing-mode incremental \
          --model-endpoint-name "non-existent-endpoint" \
          process
        ```
    *   **Expected Outcome:** The command should complete, but the output should show that one file failed to process.

4.  **Check for Failed Files:**
    *   Use the `failed` subcommand to see the list of failed files.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.main \
          --catalog <your_catalog> \
          --schema <your_schema> \
          ocr \
          failed
        ```
    *   **Expected Outcome:** The output should list `example2.pdf` as a failed file, along with an error message indicating the model endpoint could not be reached or found.

5.  **Reset Failed Files:**
    *   Use the `reset-failed` subcommand to reset the status of the failed file so it can be reprocessed.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.main \
          --catalog <your_catalog> \
          --schema <your_schema> \
          ocr \
          reset-failed
        ```
    *   **Expected Outcome:** The output should indicate that one file was reset.

6.  **Reprocess with Correct Endpoint:**
    *   Run the OCR process again. This time, it will use the default, correct endpoint name, pick up the reset file, and process it successfully.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.main \
          --catalog <your_catalog> \
          --schema <your_schema> \
          ocr \
          --processing-mode incremental \
          process
        ```
    *   **Expected Outcome:** The command should complete successfully, and the output should show that one file was processed successfully.

7.  **Verify Reprocessing:**
    *   Query the `pdf_ocr_results` table. It should now contain the extracted text from `example2.pdf`.
    *   Query the `pdf_processing_state` table. It should show that `example2.pdf` was processed successfully.

## Scenario 3: Testing the Claude Connection

This scenario tests the direct connection to the configured Claude model serving endpoint.

**Prerequisites:**

*   A running Databricks Model Serving endpoint for Claude that your user or service principal has permission to query.

**Steps:**

1.  **Run the Claude Test Command:**
    *   This command sends a simple test request to the Claude API to verify the connection and credentials.
    *   **Command:**
        ```bash
        uv run python -m databricks_pdf_ocr.main \
          --catalog <your_catalog> \
          --schema <your_schema> \
          ocr \
          test-claude
        ```
    *   **Expected Outcome:** The output should show a "SUCCESS" status and a message indicating that the connection to the Claude API is working.