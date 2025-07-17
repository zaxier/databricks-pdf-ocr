"""Unit tests for base handler classes."""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock
from pyspark.sql import SparkSession

from databricks_pdf_ocr.handlers.base import (
    PageResult,
    BatchResult,
    ProcessingResult,
    PDFHandler,
)
from databricks_pdf_ocr.config.settings import ProcessingConfig


class TestPageResult:
    """Test cases for PageResult class."""

    def test_page_result_creation(self):
        """Test creating a PageResult."""
        result = PageResult(
            page_number=1,
            total_pages=10,
            extracted_text="Test text",
            extraction_confidence=0.95,
            extraction_status="success",
            processing_duration_ms=150,
        )

        assert result.page_number == 1
        assert result.total_pages == 10
        assert result.extracted_text == "Test text"
        assert result.extraction_confidence == 0.95
        assert result.extraction_status == "success"
        assert result.processing_duration_ms == 150
        assert result.error_message is None

    def test_page_result_is_success(self):
        """Test is_success method."""
        success_result = PageResult(
            page_number=1, total_pages=1, extraction_status="success"
        )
        assert success_result.is_success() is True

        failed_result = PageResult(
            page_number=1, total_pages=1, extraction_status="failed"
        )
        assert failed_result.is_success() is False

    def test_page_result_is_failed(self):
        """Test is_failed method."""
        failed_result = PageResult(
            page_number=1, total_pages=1, extraction_status="failed"
        )
        assert failed_result.is_failed() is True

        success_result = PageResult(
            page_number=1, total_pages=1, extraction_status="success"
        )
        assert success_result.is_failed() is False

    def test_page_result_with_error(self):
        """Test PageResult with error."""
        result = PageResult(
            page_number=1,
            total_pages=1,
            extraction_status="failed",
            error_message="OCR failed",
        )

        assert result.is_failed() is True
        assert result.error_message == "OCR failed"
        assert result.extracted_text is None


class TestBatchResult:
    """Test cases for BatchResult class."""

    def test_batch_result_creation(self):
        """Test creating a BatchResult."""
        result = BatchResult()

        assert result.files_processed == 0
        assert result.files_succeeded == 0
        assert result.files_failed == 0
        assert result.total_pages_processed == 0
        assert result.processing_duration_seconds == 0.0
        assert result.errors == []

    def test_batch_result_add_error(self):
        """Test adding errors to batch result."""
        result = BatchResult()
        
        result.add_error("Error 1")
        result.add_error("Error 2")

        assert len(result.errors) == 2
        assert "Error 1" in result.errors
        assert "Error 2" in result.errors

    def test_batch_result_get_success_rate(self):
        """Test calculating success rate."""
        result = BatchResult()

        # Test with no files processed
        assert result.get_success_rate() == 0.0

        # Test with some files processed
        result.files_processed = 10
        result.files_succeeded = 7
        assert result.get_success_rate() == 70.0

        # Test with all files successful
        result.files_succeeded = 10
        assert result.get_success_rate() == 100.0

    def test_batch_result_merge(self):
        """Test merging batch results."""
        result1 = BatchResult(
            files_processed=5,
            files_succeeded=4,
            files_failed=1,
            total_pages_processed=20,
            processing_duration_seconds=10.5,
            errors=["Error 1"],
        )

        result2 = BatchResult(
            files_processed=3,
            files_succeeded=2,
            files_failed=1,
            total_pages_processed=15,
            processing_duration_seconds=5.5,
            errors=["Error 2", "Error 3"],
        )

        merged = result1.merge(result2)

        assert merged.files_processed == 8
        assert merged.files_succeeded == 6
        assert merged.files_failed == 2
        assert merged.total_pages_processed == 35
        assert merged.processing_duration_seconds == 16.0
        assert len(merged.errors) == 3
        assert "Error 1" in merged.errors
        assert "Error 2" in merged.errors
        assert "Error 3" in merged.errors


class TestProcessingResult:
    """Test cases for ProcessingResult class."""

    def test_processing_result_creation(self):
        """Test creating a ProcessingResult."""
        run_id = "test_run_123"
        start_time = datetime.now()
        config = {"key": "value"}

        result = ProcessingResult(
            run_id=run_id,
            start_time=start_time,
            configuration=config,
        )

        assert result.run_id == run_id
        assert result.start_time == start_time
        assert result.end_time is None
        assert result.batch_results == []
        assert result.total_files_processed == 0
        assert result.total_files_succeeded == 0
        assert result.total_files_failed == 0
        assert result.total_pages_processed == 0
        assert result.total_duration_seconds == 0.0
        assert result.configuration == config

    def test_add_batch_result(self):
        """Test adding batch results."""
        processing_result = ProcessingResult(
            run_id="test", start_time=datetime.now()
        )

        batch1 = BatchResult(
            files_processed=5,
            files_succeeded=4,
            files_failed=1,
            total_pages_processed=20,
            processing_duration_seconds=10.0,
        )

        batch2 = BatchResult(
            files_processed=3,
            files_succeeded=3,
            files_failed=0,
            total_pages_processed=12,
            processing_duration_seconds=5.0,
        )

        processing_result.add_batch_result(batch1)
        processing_result.add_batch_result(batch2)

        assert len(processing_result.batch_results) == 2
        assert processing_result.total_files_processed == 8
        assert processing_result.total_files_succeeded == 7
        assert processing_result.total_files_failed == 1
        assert processing_result.total_pages_processed == 32
        assert processing_result.total_duration_seconds == 15.0

    def test_finalize(self):
        """Test finalizing processing result."""
        start_time = datetime.now()
        result = ProcessingResult(run_id="test", start_time=start_time)

        # Add some processing time
        import time
        time.sleep(0.1)

        result.finalize()

        assert result.end_time is not None
        assert result.end_time > start_time
        assert result.total_duration_seconds > 0

    def test_get_summary(self):
        """Test getting processing summary."""
        start_time = datetime.now()
        result = ProcessingResult(
            run_id="test_run",
            start_time=start_time,
        )

        # Add batch results
        batch = BatchResult(
            files_processed=10,
            files_succeeded=8,
            files_failed=2,
            total_pages_processed=50,
        )
        result.add_batch_result(batch)
        result.finalize()

        summary = result.get_summary()

        assert summary["run_id"] == "test_run"
        assert summary["start_time"] == start_time
        assert summary["end_time"] is not None
        assert summary["files_processed"] == 10
        assert summary["files_succeeded"] == 8
        assert summary["files_failed"] == 2
        assert summary["pages_processed"] == 50
        assert summary["success_rate_percent"] == 80.0
        assert summary["batches_processed"] == 1
        assert "duration_seconds" in summary


class TestPDFHandler:
    """Test cases for PDFHandler abstract base class."""

    class ConcretePDFHandler(PDFHandler):
        """Concrete implementation for testing."""
        
        def process(self, **kwargs):
            """Test implementation of process method."""
            return ProcessingResult(
                run_id="test", start_time=datetime.now()
            )

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = Mock(spec=SparkSession)
        spark.version = "3.5.0"
        return spark

    @pytest.fixture
    def mock_config(self):
        """Create a mock ProcessingConfig."""
        config = Mock(spec=ProcessingConfig)
        config.__class__.__name__ = "ProcessingConfig"
        return config

    def test_handler_initialization(self, mock_spark, mock_config):
        """Test handler initialization."""
        handler = self.ConcretePDFHandler(mock_spark, mock_config)

        assert handler.spark == mock_spark
        assert handler.config == mock_config

    def test_validate_config_valid(self, mock_spark, mock_config):
        """Test config validation with valid inputs."""
        handler = self.ConcretePDFHandler(mock_spark, mock_config)
        
        # Should not raise any exception
        handler.validate_config()

    def test_validate_config_invalid_spark(self, mock_config):
        """Test config validation with invalid spark session."""
        handler = self.ConcretePDFHandler("not_a_spark_session", mock_config)

        with pytest.raises(ValueError, match="spark must be a valid SparkSession"):
            handler.validate_config()

    def test_validate_config_invalid_config(self, mock_spark):
        """Test config validation with invalid config."""
        handler = self.ConcretePDFHandler(mock_spark, "not_a_config")

        with pytest.raises(ValueError, match="config must be a ProcessingConfig instance"):
            handler.validate_config()

    def test_get_handler_info(self, mock_spark, mock_config):
        """Test getting handler information."""
        handler = self.ConcretePDFHandler(mock_spark, mock_config)
        info = handler.get_handler_info()

        assert info["handler_type"] == "ConcretePDFHandler"
        assert info["spark_version"] == "3.5.0"
        assert info["config_type"] == "ProcessingConfig"

    def test_log_processing_start(self, mock_spark, mock_config, capsys):
        """Test logging processing start."""
        handler = self.ConcretePDFHandler(mock_spark, mock_config)
        
        handler.log_processing_start(
            "test_run_123",
            mode="incremental",
            batch_size=10
        )

        captured = capsys.readouterr()
        assert "[ConcretePDFHandler] Starting processing run: test_run_123" in captured.out
        assert "mode: incremental" in captured.out
        assert "batch_size: 10" in captured.out

    def test_log_processing_end(self, mock_spark, mock_config, capsys):
        """Test logging processing end."""
        handler = self.ConcretePDFHandler(mock_spark, mock_config)
        
        result = ProcessingResult(
            run_id="test_run",
            start_time=datetime.now(),
        )
        result.total_files_processed = 100
        result.total_files_succeeded = 95
        result.total_pages_processed = 500
        result.finalize()

        handler.log_processing_end(result)

        captured = capsys.readouterr()
        assert "[ConcretePDFHandler] Completed processing run: test_run" in captured.out
        assert "Duration:" in captured.out
        assert "Files processed: 100" in captured.out
        assert "Success rate: 95.0%" in captured.out
        assert "Pages processed: 500" in captured.out

    def test_handle_error(self, mock_spark, mock_config, capsys):
        """Test error handling."""
        handler = self.ConcretePDFHandler(mock_spark, mock_config)
        
        test_error = ValueError("Test error message")

        with pytest.raises(ValueError):
            handler.handle_error(test_error, "test context")

        captured = capsys.readouterr()
        assert "ERROR: Error in ConcretePDFHandler (test context): Test error message" in captured.out

    def test_abstract_method_not_implemented(self, mock_spark, mock_config):
        """Test that abstract method must be implemented."""
        with pytest.raises(TypeError):
            # This should fail because PDFHandler is abstract
            PDFHandler(mock_spark, mock_config)