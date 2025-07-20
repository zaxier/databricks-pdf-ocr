"""OCR processor for PDF text extraction."""

import io
import uuid
from datetime import datetime

import fitz  # type: ignore[import-untyped]
from PIL import Image
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from ..clients.claude import ClaudeClient
from ..config import OCRProcessingConfig
from ..schemas import get_target_schema


class OCRProcessor:
    """Processes PDFs for OCR text extraction."""

    def __init__(
        self, spark: SparkSession, config: OCRProcessingConfig, claude_client: ClaudeClient
    ):
        self.spark = spark
        self.config = config
        self.claude_client = claude_client

    def pdf_to_images(self, pdf_content: bytes, dpi: int | None = None) -> list[bytes]:
        """Convert PDF to list of PNG images."""
        if dpi is None:
            dpi = self.claude_client.config.image_dpi

        images = []

        try:
            doc = fitz.open(stream=pdf_content, filetype="pdf")

            for page_num in range(len(doc)):
                page = doc.load_page(page_num)

                mat = fitz.Matrix(dpi / 72, dpi / 72)
                pix = page.get_pixmap(matrix=mat)

                img_data = pix.tobytes("png")
                img = Image.open(io.BytesIO(img_data))

                img_bytes = io.BytesIO()
                img.save(img_bytes, format="PNG")
                images.append(img_bytes.getvalue())

            doc.close()

        except Exception as e:
            raise ValueError(f"Failed to convert PDF to images: {str(e)}") from e

        return images

    def get_unprocessed_files(self) -> list:
        """Get list of files that haven't been processed yet."""
        if self.config.processing_mode == "incremental":
            # Find files not in results table
            source_df = self.spark.table(self.config.source_table_path)

            try:
                results_df = self.spark.table(self.config.target_table_path)
                processed_file_ids = [
                    row.file_id for row in results_df.select("file_id").distinct().collect()
                ]
                unprocessed_df = source_df.filter(~col("file_id").isin(processed_file_ids))
            except Exception:
                # Results table doesn't exist yet, process all files
                unprocessed_df = source_df

        elif self.config.processing_mode == "reprocess_all":
            unprocessed_df = self.spark.table(self.config.source_table_path)

        elif self.config.processing_mode == "reprocess_specific":
            unprocessed_df = self.spark.table(self.config.source_table_path).filter(
                col("file_id").isin(self.config.specific_file_ids)
            )
        else:
            raise ValueError(f"Unknown processing mode: {self.config.processing_mode}")

        return unprocessed_df.limit(self.config.max_docs_per_run).collect()

    def process_single_pdf(self, file_row) -> list[dict]:  # type: ignore[no-untyped-def]
        """Process a single PDF file and return page results."""
        print(f"Processing: {file_row.file_name}")

        try:
            images = self.pdf_to_images(file_row.file_content)

            if not images:
                raise ValueError("No images extracted from PDF")

            # Limit pages if configured
            if self.config.max_pages_per_pdf:
                images = images[: self.config.max_pages_per_pdf]

            page_results = []
            for page_num, image_data in enumerate(images, 1):
                print(f"  Processing page {page_num}/{len(images)}")

                ocr_result = self.claude_client.extract_text_from_image(image_data)

                page_result = {
                    "result_id": str(uuid.uuid4()),
                    "file_id": file_row.file_id,
                    "page_number": page_num,
                    "total_pages": len(images),
                    "extracted_text": ocr_result.get("extracted_text"),
                    "extraction_confidence": ocr_result.get("confidence_score"),
                    "processing_timestamp": datetime.now(),
                    "processing_duration_ms": ocr_result.get("processing_duration_ms", 0),
                    "ocr_model": ocr_result.get("model", "unknown"),
                    "extraction_status": ocr_result.get("status", "failed"),
                    "error_message": ocr_result.get("error"),
                }
                page_results.append(page_result)

            return page_results

        except Exception as e:
            # Return error result for the file
            return [
                {
                    "result_id": str(uuid.uuid4()),
                    "file_id": file_row.file_id,
                    "page_number": 1,
                    "total_pages": 1,
                    "extracted_text": None,
                    "extraction_confidence": None,
                    "processing_timestamp": datetime.now(),
                    "processing_duration_ms": 0,
                    "ocr_model": self.claude_client.config.endpoint_name,
                    "extraction_status": "failed",
                    "error_message": str(e),
                }
            ]

    def process_batch(self) -> dict:
        """Process a batch of PDFs and return processing statistics."""
        print(f"Starting OCR processing batch (mode: {self.config.processing_mode})")

        unprocessed_files = self.get_unprocessed_files()

        if not unprocessed_files:
            print("No files to process")
            return {
                "files_processed": 0,
                "files_succeeded": 0,
                "files_failed": 0,
                "total_pages_processed": 0,
            }

        print(f"Found {len(unprocessed_files)} files to process")

        all_results = []
        files_succeeded = 0
        files_failed = 0
        total_pages = 0

        for file_row in unprocessed_files:
            try:
                page_results = self.process_single_pdf(file_row)
                all_results.extend(page_results)

                # Check if any page succeeded
                if any(r["extraction_status"] == "success" for r in page_results):
                    files_succeeded += 1
                else:
                    files_failed += 1

                total_pages += len(page_results)

            except Exception as e:
                print(f"Error processing {file_row.file_name}: {str(e)}")
                files_failed += 1

        # Save all results to target table
        if all_results:
            results_df = self.spark.createDataFrame(all_results, schema=get_target_schema())
            results_df.write.mode("append").saveAsTable(self.config.target_table_path)

        stats = {
            "files_processed": len(unprocessed_files),
            "files_succeeded": files_succeeded,
            "files_failed": files_failed,
            "total_pages_processed": total_pages,
        }

        print(f"Processing completed: {stats}")
        return stats
