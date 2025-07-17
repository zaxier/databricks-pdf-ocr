"""Unit tests for OCR processor handler."""

from unittest.mock import Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row

from databricks_pdf_ocr.config.settings import ProcessingMode
from databricks_pdf_ocr.handlers.base import BatchResult, PageResult
from databricks_pdf_ocr.handlers.ocr_processor import OCRProcessor


class TestOCRProcessor:
    """Test cases for OCRProcessor class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = Mock(spec=SparkSession)
        spark.version = "3.5.0"
        spark.createDataFrame = Mock()
        return spark

    @pytest.fixture
    def mock_config(self):
        """Create a mock OCRConfig."""
        config = Mock()
        config.__class__.__name__ = "OCRConfig"
        config.catalog = "test_catalog"
        config.schema = "test_schema"
        config.source_table_name = "test_catalog.test_schema.pdf_source"
        config.target_table_name = "test_catalog.test_schema.pdf_ocr_results"
        config.model_endpoint_name = "claude-3-haiku"
        config.claude_max_tokens = 8000
        config.claude_temperature = 0.0
        config.max_retries = 3
        config.retry_delay_seconds = 1
        config.processing_mode = ProcessingMode.INCREMENTAL
        config.max_docs_per_run = 100
        config.batch_size = 10
        config.specific_file_ids = None
        config.max_pages_per_pdf = None
        config.image_dpi = 150
        config.image_max_edge_pixels = 1568
        config.to_dict = Mock(return_value={"test": "config"})
        return config

    @pytest.fixture
    def mock_state_manager(self):
        """Create a mock StateManager."""
        state_manager = Mock()
        state_manager.create_run_id.return_value = "test_run_123"
        state_manager.get_pending_files = Mock()
        state_manager.update_multiple_file_statuses = Mock()
        state_manager.update_file_status = Mock()
        state_manager.record_run_metrics = Mock()
        state_manager.get_processing_stats = Mock()
        state_manager.get_failed_files = Mock()
        state_manager.reset_file_status = Mock()
        return state_manager

    @pytest.fixture
    def mock_claude_client(self):
        """Create a mock ClaudeClient."""
        client = Mock()
        client.extract_text_from_image = Mock()
        client.test_connection = Mock()
        return client

    @pytest.fixture
    def sample_pdf_row(self):
        """Create a sample PDF row."""
        return Row(
            file_id="file_123",
            file_name="test.pdf",
            file_content=b"sample_pdf_content",
            file_size=1024,
            processing_status="pending"
        )

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_handler_initialization(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test handler initialization."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager.return_value = mock_state_manager

        handler = OCRProcessor(mock_spark, mock_config)

        assert handler.spark == mock_spark
        assert handler.config == mock_config
        assert handler.state_manager is not None
        assert handler.claude_client is not None

        # Verify ClaudeClient was initialized with correct parameters
        mock_claude_client.assert_called_once_with(
            endpoint_name=mock_config.model_endpoint_name,
            max_tokens=mock_config.claude_max_tokens,
            temperature=mock_config.claude_temperature,
            max_retries=mock_config.max_retries,
            retry_delay=mock_config.retry_delay_seconds,
        )

        # Verify tables were ensured to exist
        mock_ensure_tables.assert_called_once_with(
            mock_spark,
            mock_config.catalog,
            mock_config.schema
        )

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_process_no_files(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test processing when no files are available."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager_instance = Mock()
        mock_state_manager_instance.create_run_id.return_value = "test_run_123"
        mock_state_manager.return_value = mock_state_manager_instance

        # Mock no files to process
        mock_pending_files = Mock()
        mock_pending_files.collect.return_value = []
        mock_state_manager_instance.get_pending_files.return_value = mock_pending_files

        handler = OCRProcessor(mock_spark, mock_config)

        result = handler.process()

        assert result.run_id == "test_run_123"
        assert result.total_files_processed == 0
        assert result.total_files_succeeded == 0
        assert result.total_files_failed == 0

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_process_with_files(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config, sample_pdf_row):
        """Test processing with files available."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager_instance = Mock()
        mock_state_manager_instance.create_run_id.return_value = "test_run_123"
        mock_state_manager.return_value = mock_state_manager_instance

        # Mock files to process
        mock_pending_files = Mock()
        mock_pending_files.collect.return_value = [sample_pdf_row]
        mock_state_manager_instance.get_pending_files.return_value = mock_pending_files

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock the batch processing
        mock_batch_result = BatchResult(
            files_processed=1,
            files_succeeded=1,
            files_failed=0,
            total_pages_processed=3
        )
        handler._process_batch = Mock(return_value=mock_batch_result)

        result = handler.process()

        assert result.run_id == "test_run_123"
        assert result.total_files_processed == 1
        assert result.total_files_succeeded == 1
        assert result.total_files_failed == 0
        assert result.total_pages_processed == 3

        # Verify batch processing was called
        handler._process_batch.assert_called_once_with([sample_pdf_row], "test_run_123")

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_process_batch(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config, sample_pdf_row):
        """Test processing a batch of files."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager_instance = mock_state_manager
        mock_state_manager.return_value = mock_state_manager_instance

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock single PDF processing
        mock_page_results = [
            PageResult(page_number=1, total_pages=2, extraction_status="success", extracted_text="Page 1 text"),
            PageResult(page_number=2, total_pages=2, extraction_status="success", extracted_text="Page 2 text")
        ]
        handler._process_single_pdf = Mock(return_value=mock_page_results)
        handler._store_page_results = Mock()

        result = handler._process_batch([sample_pdf_row], "test_run_123")

        assert result.files_processed == 1
        assert result.files_succeeded == 1
        assert result.files_failed == 0
        assert result.total_pages_processed == 2

        # Verify file status was updated
        mock_state_manager_instance.update_file_status.assert_called_with(
            sample_pdf_row.file_id, "completed"
        )

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_process_batch_with_failure(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config, sample_pdf_row):
        """Test processing a batch with a failed file."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager_instance = mock_state_manager
        mock_state_manager.return_value = mock_state_manager_instance

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock single PDF processing to fail
        handler._process_single_pdf = Mock(side_effect=Exception("PDF processing failed"))

        result = handler._process_batch([sample_pdf_row], "test_run_123")

        assert result.files_processed == 1
        assert result.files_succeeded == 0
        assert result.files_failed == 1
        assert len(result.errors) == 1
        assert "PDF processing failed" in result.errors[0]

        # Verify file status was updated to failed
        mock_state_manager_instance.update_file_status.assert_called_with(
            sample_pdf_row.file_id, "failed", "Error processing test.pdf: PDF processing failed"
        )

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.fitz')
    def test_process_single_pdf(self, mock_fitz, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config, sample_pdf_row):
        """Test processing a single PDF file."""
        mock_claude_client_instance = mock_claude_client
        mock_claude_client.return_value = mock_claude_client_instance
        mock_state_manager.return_value = mock_state_manager

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock PDF to images conversion
        mock_images = [b"image1_data", b"image2_data"]
        handler._pdf_to_images = Mock(return_value=mock_images)

        # Mock Claude client responses
        mock_claude_client_instance.extract_text_from_image.side_effect = [
            {"extracted_text": "Page 1 text", "confidence_score": 0.95, "status": "success"},
            {"extracted_text": "Page 2 text", "confidence_score": 0.92, "status": "success"}
        ]

        result = handler._process_single_pdf(sample_pdf_row)

        assert len(result) == 2
        assert result[0].page_number == 1
        assert result[0].extracted_text == "Page 1 text"
        assert result[0].extraction_confidence == 0.95
        assert result[0].extraction_status == "success"
        assert result[1].page_number == 2
        assert result[1].extracted_text == "Page 2 text"

        # Verify Claude client was called for each page
        assert mock_claude_client_instance.extract_text_from_image.call_count == 2

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.fitz')
    def test_process_single_pdf_with_page_limit(self, mock_fitz, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config, sample_pdf_row):
        """Test processing a single PDF with page limit."""
        mock_claude_client_instance = mock_claude_client
        mock_claude_client.return_value = mock_claude_client_instance
        mock_state_manager.return_value = mock_state_manager

        # Set page limit
        mock_config.max_pages_per_pdf = 2

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock PDF to images conversion with more pages than limit
        mock_images = [b"image1_data", b"image2_data", b"image3_data", b"image4_data"]
        handler._pdf_to_images = Mock(return_value=mock_images)

        # Mock Claude client responses
        mock_claude_client_instance.extract_text_from_image.side_effect = [
            {"extracted_text": "Page 1 text", "confidence_score": 0.95, "status": "success"},
            {"extracted_text": "Page 2 text", "confidence_score": 0.92, "status": "success"}
        ]

        result = handler._process_single_pdf(sample_pdf_row)

        # Should only process first 2 pages due to limit
        assert len(result) == 2
        assert mock_claude_client_instance.extract_text_from_image.call_count == 2

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.fitz')
    def test_pdf_to_images(self, mock_fitz, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test converting PDF to images."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager.return_value = mock_state_manager

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock fitz (PyMuPDF) objects
        mock_doc = Mock()
        mock_page = Mock()
        mock_pixmap = Mock()

        mock_fitz.open.return_value = mock_doc
        mock_doc.__len__ = Mock(return_value=2)
        mock_doc.load_page.return_value = mock_page
        mock_page.get_pixmap.return_value = mock_pixmap
        mock_pixmap.tobytes.return_value = b"png_image_data"

        # Mock PIL Image
        with patch('databricks_pdf_ocr.handlers.ocr_processor.Image') as mock_image:
            mock_img = Mock()
            mock_img.size = (1000, 800)  # Within limits
            mock_image.open.return_value = mock_img

            # Mock BytesIO for image saving
            with patch('databricks_pdf_ocr.handlers.ocr_processor.io.BytesIO') as mock_bytesio:
                mock_bytes_obj = Mock()
                mock_bytes_obj.getvalue.return_value = b"final_png_data"
                mock_bytesio.return_value = mock_bytes_obj

                pdf_content = b"fake_pdf_content"
                result = handler._pdf_to_images(pdf_content)

                assert len(result) == 2
                assert result[0] == b"final_png_data"
                assert result[1] == b"final_png_data"

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_resize_image(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test resizing image to fit within max edge pixels."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager.return_value = mock_state_manager

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock PIL Image
        mock_img = Mock()
        mock_img.size = (2000, 1500)  # Larger than max_edge_pixels (1568)
        mock_resized = Mock()
        mock_img.resize.return_value = mock_resized

        result = handler._resize_image(mock_img, 1568)

        # Should resize to fit within max edge pixels
        mock_img.resize.assert_called_once()
        assert result == mock_resized

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_store_page_results(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test storing page results in database."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager.return_value = mock_state_manager

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock page results
        page_results = [
            PageResult(
                page_number=1,
                total_pages=2,
                extracted_text="Page 1 text",
                extraction_confidence=0.95,
                extraction_status="success",
                processing_duration_ms=150
            ),
            PageResult(
                page_number=2,
                total_pages=2,
                extracted_text="Page 2 text",
                extraction_confidence=0.92,
                extraction_status="success",
                processing_duration_ms=200
            )
        ]

        # Mock DataFrame creation and writing
        mock_df = Mock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_df.write = Mock()

        handler._store_page_results(page_results, "file_123", "run_123")

        # Verify DataFrame was created and written
        mock_spark.createDataFrame.assert_called_once()
        mock_df.write.mode.assert_called_once_with("append")

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_process_specific_files(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test processing specific files by ID."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager.return_value = mock_state_manager

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock the process method
        handler.process = Mock()

        original_mode = mock_config.processing_mode
        original_file_ids = mock_config.specific_file_ids

        file_ids = ["file1", "file2", "file3"]
        handler.process_specific_files(file_ids)

        # Verify config was restored back to original after processing
        assert mock_config.processing_mode == original_mode
        assert mock_config.specific_file_ids == original_file_ids

        # Verify process was called
        handler.process.assert_called_once()

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_get_processing_stats(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test getting processing statistics."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager_instance = mock_state_manager
        mock_state_manager.return_value = mock_state_manager_instance

        expected_stats = {"total_files": 100, "pending": 20, "completed": 75, "failed": 5}
        mock_state_manager_instance.get_processing_stats.return_value = expected_stats

        handler = OCRProcessor(mock_spark, mock_config)

        result = handler.get_processing_stats()

        assert result == expected_stats
        mock_state_manager_instance.get_processing_stats.assert_called_once()

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_get_failed_files(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test getting failed files."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager_instance = mock_state_manager
        mock_state_manager.return_value = mock_state_manager_instance

        mock_failed_files = Mock(spec=DataFrame)
        mock_state_manager_instance.get_failed_files.return_value = mock_failed_files

        handler = OCRProcessor(mock_spark, mock_config)

        result = handler.get_failed_files(limit=10)

        assert result == mock_failed_files
        mock_state_manager_instance.get_failed_files.assert_called_once_with(10)

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_reset_failed_files(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test resetting failed files to pending status."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager_instance = mock_state_manager
        mock_state_manager.return_value = mock_state_manager_instance

        handler = OCRProcessor(mock_spark, mock_config)

        # Mock failed files
        mock_failed_files = Mock()
        mock_failed_files.collect.return_value = [
            Row(file_id="file1"),
            Row(file_id="file2"),
            Row(file_id="file3")
        ]
        handler.get_failed_files = Mock(return_value=mock_failed_files)

        result = handler.reset_failed_files()

        assert result == 3
        mock_state_manager_instance.reset_file_status.assert_called_once_with(
            ["file1", "file2", "file3"]
        )

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_test_claude_connection(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test testing Claude connection."""
        mock_claude_client_instance = mock_claude_client
        mock_claude_client.return_value = mock_claude_client_instance
        mock_state_manager.return_value = mock_state_manager

        expected_result = {"status": "connected", "endpoint": "claude-3-haiku"}
        mock_claude_client_instance.test_connection.return_value = expected_result

        handler = OCRProcessor(mock_spark, mock_config)

        result = handler.test_claude_connection()

        assert result == expected_result
        mock_claude_client_instance.test_connection.assert_called_once()

    @patch('databricks_pdf_ocr.schemas.tables.ensure_all_tables_exist')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.StateManager')
    @patch('databricks_pdf_ocr.handlers.ocr_processor.ClaudeClient')
    def test_get_config_dict(self, mock_claude_client, mock_state_manager, mock_ensure_tables, mock_spark, mock_config):
        """Test getting configuration as dictionary."""
        mock_claude_client.return_value = mock_claude_client
        mock_state_manager.return_value = mock_state_manager

        handler = OCRProcessor(mock_spark, mock_config)

        result = handler._get_config_dict()

        assert result == {"test": "config"}
        mock_config.to_dict.assert_called_once()
