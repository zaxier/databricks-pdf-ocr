"""Batch processing example for multiple PDFs."""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv, dotenv_values

from pdf_parse_claude import DatabricksClaudeOCR


def process_single_pdf(ocr_client, pdf_path, output_dir):
    """Process a single PDF file."""
    try:
        print(f"Processing: {pdf_path}")

        # Extract text
        extracted_text = ocr_client.extract_text_from_pdf(str(pdf_path))

        # Save to output directory
        output_file = output_dir / f"{pdf_path.stem}_extracted.txt"
        output_file.write_text(extracted_text, encoding="utf-8")

        print(f"✓ Completed: {pdf_path.name} -> {output_file.name}")
        return pdf_path, True, None

    except Exception as e:
        print(f"✗ Failed: {pdf_path.name} - {str(e)}")
        return pdf_path, False, str(e)


def batch_process_pdfs(input_dir="./data", output_dir="./output", max_workers=3):
    """Process all PDFs in a directory."""
    # Setup paths
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Load environment variables from .env file
    load_dotenv()
    
    # Force use of .env file values to override system environment variables
    config = dotenv_values(".env")
    databricks_host = config.get("DATABRICKS_HOST", "https://your-workspace.databricks.com")
    databricks_token = config.get("DATABRICKS_ACCESS_TOKEN", "your-token-here")
    
    print(f"Using credentials from .env file:")
    print(f"  Host: {databricks_host}")
    print(f"  Token: {databricks_token[:15]}..." if databricks_token else "  Token: Not set")
    
    # Initialize OCR client
    ocr = DatabricksClaudeOCR(
        workspace_url=databricks_host,
        token=databricks_token
    )

    # Find all PDF files
    pdf_files = list(input_path.glob("*.pdf"))
    if not pdf_files:
        print(f"No PDF files found in {input_dir}")
        return

    print(f"Found {len(pdf_files)} PDF files to process")

    # Process PDFs in parallel
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_pdf = {
            executor.submit(process_single_pdf, ocr, pdf, output_path): pdf
            for pdf in pdf_files
        }

        # Collect results as they complete
        for future in as_completed(future_to_pdf):
            result = future.result()
            results.append(result)

    # Summary
    successful = sum(1 for _, success, _ in results if success)
    failed = len(results) - successful

    print("\nBatch processing complete:")
    print(f"  ✓ Successful: {successful}")
    print(f"  ✗ Failed: {failed}")

    if failed > 0:
        print("\nFailed files:")
        for pdf, success, error in results:
            if not success:
                print(f"  - {pdf.name}: {error}")

    return results


def main():
    """Main batch processing example."""
    # Process all PDFs in the data directory
    results = batch_process_pdfs(
        input_dir="./data",
        output_dir="./output",
        max_workers=3  # Adjust based on your API rate limits
    )


if __name__ == "__main__":
    main()
