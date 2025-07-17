from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

from ..config.settings import ProcessingConfig


@dataclass
class PageResult:
    """Result of processing a single page"""

    page_number: int
    total_pages: int
    extracted_text: str | None = None
    extraction_confidence: float | None = None
    extraction_status: str = "pending"  # 'success', 'failed', 'partial'
    error_message: str | None = None
    processing_duration_ms: int = 0

    def is_success(self) -> bool:
        """Check if extraction was successful"""
        return self.extraction_status == "success"

    def is_failed(self) -> bool:
        """Check if extraction failed"""
        return self.extraction_status == "failed"


@dataclass
class BatchResult:
    """Result of processing a batch of PDFs"""

    files_processed: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    total_pages_processed: int = 0
    processing_duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    def add_error(self, error: str) -> None:
        """Add an error to the batch result"""
        self.errors.append(error)

    def get_success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.files_processed == 0:
            return 0.0
        return (self.files_succeeded / self.files_processed) * 100

    def merge(self, other: "BatchResult") -> "BatchResult":
        """Merge another batch result into this one"""
        return BatchResult(
            files_processed=self.files_processed + other.files_processed,
            files_succeeded=self.files_succeeded + other.files_succeeded,
            files_failed=self.files_failed + other.files_failed,
            total_pages_processed=self.total_pages_processed + other.total_pages_processed,
            processing_duration_seconds=self.processing_duration_seconds
            + other.processing_duration_seconds,
            errors=self.errors + other.errors,
        )


@dataclass
class ProcessingResult:
    """Result of a complete processing operation"""

    run_id: str
    start_time: datetime
    end_time: datetime | None = None
    batch_results: list[BatchResult] = field(default_factory=list)
    total_files_processed: int = 0
    total_files_succeeded: int = 0
    total_files_failed: int = 0
    total_pages_processed: int = 0
    total_duration_seconds: float = 0.0
    configuration: dict[str, Any] = field(default_factory=dict)

    def add_batch_result(self, batch_result: BatchResult) -> None:
        """Add a batch result to the processing result"""
        self.batch_results.append(batch_result)
        self.total_files_processed += batch_result.files_processed
        self.total_files_succeeded += batch_result.files_succeeded
        self.total_files_failed += batch_result.files_failed
        self.total_pages_processed += batch_result.total_pages_processed
        self.total_duration_seconds += batch_result.processing_duration_seconds

    def finalize(self) -> None:
        """Mark the processing as complete"""
        self.end_time = datetime.now()
        if self.end_time and self.start_time:
            self.total_duration_seconds = (self.end_time - self.start_time).total_seconds()

    def get_summary(self) -> dict[str, Any]:
        """Get a summary of the processing results"""
        success_rate = 0.0
        if self.total_files_processed > 0:
            success_rate = (self.total_files_succeeded / self.total_files_processed) * 100

        return {
            "run_id": self.run_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.total_duration_seconds,
            "files_processed": self.total_files_processed,
            "files_succeeded": self.total_files_succeeded,
            "files_failed": self.total_files_failed,
            "pages_processed": self.total_pages_processed,
            "success_rate_percent": success_rate,
            "batches_processed": len(self.batch_results),
        }


class PDFHandler(ABC):
    """Abstract base class for PDF processing handlers"""

    def __init__(self, spark: SparkSession, config: ProcessingConfig):
        self.spark = spark
        self.config = config

    @abstractmethod
    def process(self, **kwargs: Any) -> ProcessingResult:
        """Process PDFs according to handler logic"""
        pass

    def validate_config(self) -> None:
        """Validate handler configuration"""
        if not isinstance(self.spark, SparkSession):
            raise ValueError("spark must be a valid SparkSession")

        if not isinstance(self.config, ProcessingConfig):
            raise ValueError("config must be a ProcessingConfig instance")

    def get_handler_info(self) -> dict[str, Any]:
        """Get information about this handler"""
        return {
            "handler_type": self.__class__.__name__,
            "spark_version": self.spark.version,
            "config_type": self.config.__class__.__name__,
        }

    def log_processing_start(self, run_id: str, **kwargs: Any) -> None:
        """Log the start of processing"""
        print(f"[{self.__class__.__name__}] Starting processing run: {run_id}")
        for key, value in kwargs.items():
            print(f"  {key}: {value}")

    def log_processing_end(self, result: ProcessingResult) -> None:
        """Log the end of processing"""
        summary = result.get_summary()
        print(f"[{self.__class__.__name__}] Completed processing run: {result.run_id}")
        print(f"  Duration: {summary['duration_seconds']:.2f}s")
        print(f"  Files processed: {summary['files_processed']}")
        print(f"  Success rate: {summary['success_rate_percent']:.1f}%")
        print(f"  Pages processed: {summary['pages_processed']}")

    def handle_error(self, error: Exception, context: str = "") -> None:
        """Handle errors during processing"""
        error_msg = f"Error in {self.__class__.__name__}"
        if context:
            error_msg += f" ({context})"
        error_msg += f": {str(error)}"

        print(f"ERROR: {error_msg}")

        # Re-raise the error to let the caller handle it
        raise error
