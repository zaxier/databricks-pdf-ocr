import io
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import fitz  # PyMuPDF # type: ignore
from PIL import Image
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row

from ..config.settings import OCRConfig, ProcessingMode, RunMetrics
from ..handlers.base import BatchResult, PageResult, PDFHandler, ProcessingResult
from ..utils.claude_client import ClaudeClient
from ..utils.state_manager import StateManager


class OCRProcessor(PDFHandler):
    """Handles OCR processing of PDF documents using Claude API"""

    def __init__(self, spark: SparkSession, config: OCRConfig):
        super().__init__(spark, config)
        self.config: OCRConfig = config
        self.state_manager = StateManager(spark, config)
        self.claude_client = ClaudeClient(
            endpoint_name=config.model_endpoint_name,
            max_tokens=config.claude_max_tokens,
            temperature=config.claude_temperature,
            max_retries=config.max_retries,
            retry_delay=config.retry_delay_seconds,
        )

        # Ensure tables exist
        self._ensure_tables_exist()

    def _ensure_tables_exist(self) -> None:
        """Ensure all required tables exist"""
        from ..schemas.tables import ensure_all_tables_exist

        ensure_all_tables_exist(self.spark, self.config.catalog, self.config.schema)

    def process(self, **kwargs: Any) -> ProcessingResult:
        """Process PDFs according to configuration"""
        run_id = self.state_manager.create_run_id()
        start_time = datetime.now()

        self.log_processing_start(
            run_id,
            mode=self.config.processing_mode,
            max_docs=self.config.max_docs_per_run,
            batch_size=self.config.batch_size,
        )

        result = ProcessingResult(
            run_id=run_id, start_time=start_time, configuration=self._get_config_dict()
        )

        try:
            # Get files to process
            files_to_process = self.state_manager.get_pending_files(
                mode=self.config.processing_mode,
                max_files=self.config.max_docs_per_run,
                specific_ids=self.config.specific_file_ids,
            )

            # Process files in batches
            files_list = files_to_process.collect()
            total_files = len(files_list)

            if total_files == 0:
                print("No files to process")
                result.finalize()
                return result

            print(f"Processing {total_files} files in batches of {self.config.batch_size}")

            # Process in batches
            for i in range(0, total_files, self.config.batch_size):
                batch_files = files_list[i : i + self.config.batch_size]
                batch_result = self._process_batch(batch_files, run_id)
                result.add_batch_result(batch_result)

                # Log batch progress
                print(
                    f"Batch {i//self.config.batch_size + 1} completed: "
                    f"{batch_result.files_succeeded}/{batch_result.files_processed} successful"
                )

            # Record final metrics
            metrics = RunMetrics(
                run_id=run_id,
                processing_mode=self.config.processing_mode,
                files_processed=result.total_files_processed,
                files_succeeded=result.total_files_succeeded,
                files_failed=result.total_files_failed,
                total_pages_processed=result.total_pages_processed,
                processing_duration_seconds=result.total_duration_seconds,
                configuration=result.configuration,
            )

            self.state_manager.record_run_metrics(metrics)

        except Exception as e:
            self.handle_error(e, "processing PDFs")
            raise
        finally:
            result.finalize()
            self.log_processing_end(result)

        return result

    def _process_batch(self, files: list[Row], run_id: str) -> BatchResult:
        """Process a batch of PDF files"""
        batch_result = BatchResult()
        batch_start_time = time.time()

        # Update all files to processing status
        file_updates = []
        for file_row in files:
            file_updates.append(
                {"file_id": file_row.file_id, "status": "processing", "increment_attempts": True}
            )

        self.state_manager.update_multiple_file_statuses(file_updates)

        # Process files concurrently
        with ThreadPoolExecutor(max_workers=min(len(files), 5)) as executor:
            future_to_file = {
                executor.submit(self._process_single_pdf, file_row): file_row for file_row in files
            }

            for future in as_completed(future_to_file):
                file_row = future_to_file[future]
                try:
                    page_results = future.result()

                    # Store results and update status
                    if page_results:
                        success_count = sum(1 for pr in page_results if pr.is_success())

                        if success_count == len(page_results):
                            status = "completed"
                        elif success_count > 0:
                            status = "completed"  # Partial success still counts as completed
                        else:
                            status = "failed"

                        self._store_page_results(page_results, file_row.file_id, run_id)
                        self.state_manager.update_file_status(file_row.file_id, status)

                        batch_result.files_succeeded += 1
                        batch_result.total_pages_processed += len(page_results)
                    else:
                        # No results - complete failure
                        error_msg = f"Failed to process PDF: {file_row.file_name}"
                        self.state_manager.update_file_status(file_row.file_id, "failed", error_msg)
                        batch_result.files_failed += 1
                        batch_result.add_error(error_msg)

                    batch_result.files_processed += 1

                except Exception as e:
                    error_msg = f"Error processing {file_row.file_name}: {str(e)}"
                    self.state_manager.update_file_status(file_row.file_id, "failed", error_msg)
                    batch_result.files_failed += 1
                    batch_result.files_processed += 1
                    batch_result.add_error(error_msg)

        batch_result.processing_duration_seconds = time.time() - batch_start_time
        return batch_result

    def _process_single_pdf(self, file_row: Row) -> list[PageResult]:
        """Process a single PDF file"""
        try:
            # Convert PDF to images
            images = self._pdf_to_images(file_row.file_content)

            if not images:
                raise ValueError("No images could be extracted from PDF")

            # Apply page limit if configured
            if self.config.max_pages_per_pdf:
                images = images[: self.config.max_pages_per_pdf]

            # Extract text from each page
            page_results = []
            total_pages = len(images)

            for page_num, image_data in enumerate(images, 1):
                try:
                    start_time = time.time()

                    # Call Claude API for OCR
                    ocr_result = self.claude_client.extract_text_from_image(
                        image_data, image_format="PNG"
                    )

                    processing_time = int((time.time() - start_time) * 1000)

                    # Create page result
                    page_result = PageResult(
                        page_number=page_num,
                        total_pages=total_pages,
                        extracted_text=ocr_result.get("extracted_text"),
                        extraction_confidence=ocr_result.get("confidence_score"),
                        extraction_status=ocr_result.get("status", "failed"),
                        error_message=ocr_result.get("error"),
                        processing_duration_ms=processing_time,
                    )

                    page_results.append(page_result)

                except Exception as e:
                    # Handle individual page failure
                    page_results.append(
                        PageResult(
                            page_number=page_num,
                            total_pages=total_pages,
                            extraction_status="failed",
                            error_message=str(e),
                        )
                    )

            return page_results

        except Exception as e:
            # Return empty list to indicate complete failure
            print(f"Failed to process PDF {file_row.file_name}: {str(e)}")
            return []

    def _pdf_to_images(self, pdf_content: bytes) -> list[bytes]:
        """Convert PDF content to list of PNG images"""
        images = []

        try:
            # Open PDF from bytes
            doc = fitz.open(stream=pdf_content, filetype="pdf")

            for page_num in range(len(doc)):
                page = doc.load_page(page_num)

                # Convert to image with specified DPI
                mat = fitz.Matrix(self.config.image_dpi / 72, self.config.image_dpi / 72)
                pix = page.get_pixmap(matrix=mat)

                # Convert to PIL Image
                img_data = pix.tobytes("png")
                img: Image.Image = Image.open(io.BytesIO(img_data))

                # Resize if needed to respect max_edge_pixels
                if max(img.size) > self.config.image_max_edge_pixels:
                    img = self._resize_image(img, self.config.image_max_edge_pixels)

                # Convert back to bytes
                img_bytes = io.BytesIO()
                img.save(img_bytes, format="PNG")
                images.append(img_bytes.getvalue())

            doc.close()

        except Exception as e:
            raise ValueError(f"Failed to convert PDF to images: {str(e)}") from e

        return images

    def _resize_image(self, img: Image.Image | Any, max_edge_pixels: int) -> Image.Image:
        """Resize image to fit within max edge pixels while maintaining aspect ratio"""
        width, height = img.size
        max_dimension = max(width, height)

        if max_dimension <= max_edge_pixels:
            return img

        # Calculate new dimensions
        scale_factor = max_edge_pixels / max_dimension
        new_width = int(width * scale_factor)
        new_height = int(height * scale_factor)

        return img.resize((new_width, new_height), Image.Resampling.LANCZOS)

    def _store_page_results(
        self, page_results: list[PageResult], file_id: str, run_id: str
    ) -> None:
        """Store page results in the target table"""
        if not page_results:
            return

        # Prepare data for insertion
        results_data = []
        for page_result in page_results:
            results_data.append(
                {
                    "result_id": str(uuid.uuid4()),
                    "file_id": str(file_id),
                    "page_number": int(page_result.page_number),
                    "total_pages": int(page_result.total_pages),
                    "extracted_text": page_result.extracted_text,
                    "extraction_confidence": (
                        float(page_result.extraction_confidence)
                        if page_result.extraction_confidence is not None
                        else None
                    ),
                    "processing_timestamp": datetime.now(),
                    "processing_duration_ms": (
                        int(page_result.processing_duration_ms)
                        if page_result.processing_duration_ms is not None
                        else 0
                    ),
                    "ocr_model": str(self.config.model_endpoint_name),
                    "extraction_status": str(page_result.extraction_status),
                    "error_message": page_result.error_message,
                }
            )

        # Create DataFrame with explicit schema to avoid type inference issues
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("result_id", StringType(), False),
                StructField("file_id", StringType(), False),
                StructField("page_number", IntegerType(), False),
                StructField("total_pages", IntegerType(), False),
                StructField("extracted_text", StringType(), True),
                StructField("extraction_confidence", DoubleType(), True),
                StructField("processing_timestamp", TimestampType(), False),
                StructField("processing_duration_ms", LongType(), False),
                StructField("ocr_model", StringType(), False),
                StructField("extraction_status", StringType(), False),
                StructField("error_message", StringType(), True),
            ]
        )

        results_df = self.spark.createDataFrame(results_data, schema)
        results_df.write.mode("append").saveAsTable(self.config.target_table_name)

    def _get_config_dict(self) -> dict[str, Any]:
        """Get configuration as dictionary for logging"""
        return self.config.to_dict()

    def process_specific_files(self, file_ids: list[str]) -> ProcessingResult:
        """Process specific files by ID"""
        # Update config to reprocess specific files
        original_mode = self.config.processing_mode
        original_file_ids = self.config.specific_file_ids

        self.config.processing_mode = ProcessingMode.REPROCESS_SPECIFIC
        self.config.specific_file_ids = file_ids

        try:
            return self.process()
        finally:
            # Restore original configuration
            self.config.processing_mode = original_mode
            self.config.specific_file_ids = original_file_ids

    def get_processing_stats(self) -> dict[str, Any]:
        """Get current processing statistics"""
        return self.state_manager.get_processing_stats()

    def get_failed_files(self, limit: int | None = None) -> DataFrame:
        """Get files that have failed processing"""
        return self.state_manager.get_failed_files(limit)

    def reset_failed_files(self) -> int:
        """Reset all failed files to pending status"""
        failed_files = self.get_failed_files()
        failed_file_ids = [row.file_id for row in failed_files.collect()]

        if failed_file_ids:
            self.state_manager.reset_file_status(failed_file_ids)

        return len(failed_file_ids)

    def test_claude_connection(self) -> dict[str, Any]:
        """Test connection to Claude API"""
        return self.claude_client.test_connection()
