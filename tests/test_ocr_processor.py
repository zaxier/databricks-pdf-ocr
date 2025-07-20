"""Tests for OCR processor."""

from unittest.mock import Mock, patch

import pytest

from databricks_pdf_ocr.clients.claude import ClaudeClient
from databricks_pdf_ocr.config import OCRProcessingConfig
from databricks_pdf_ocr.processors.ocr import OCRProcessor


class TestOCRProcessor:
    """Test cases for OCRProcessor."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        return Mock()

    @pytest.fixture
    def mock_config(self):
        """Mock OCR processing config."""
        config = Mock(spec=OCRProcessingConfig)
        config.processing_mode = "incremental"
        config.max_docs_per_run = 10
        config.max_pages_per_pdf = None
        config.source_table_path = "test.pdf_source"
        config.target_table_path = "test.pdf_results"
        config.max_file_size_mb = 50
        config.max_file_size_bytes = 50 * 1024 * 1024
        return config

    @pytest.fixture
    def mock_claude_client(self):
        """Mock Claude client."""
        client = Mock(spec=ClaudeClient)
        client.config = Mock()
        client.config.image_dpi = 150
        client.config.endpoint_name = "claude-3-sonnet"
        return client

    @pytest.fixture
    def ocr_processor(self, mock_spark, mock_config, mock_claude_client):
        """Create OCRProcessor instance with mocks."""
        return OCRProcessor(mock_spark, mock_config, mock_claude_client)

    def test_pdf_to_images_success(self, ocr_processor):
        """Test successful PDF to images conversion."""
        mock_pdf_content = b"fake_pdf_content"

        with patch("fitz.open") as mock_fitz_open:
            # Setup mock document
            mock_doc = Mock()
            mock_doc.__len__ = Mock(return_value=2)
            mock_page = Mock()
            mock_pixmap = Mock()
            mock_pixmap.tobytes.return_value = b"fake_png_data"
            mock_page.get_pixmap.return_value = mock_pixmap
            mock_doc.load_page.return_value = mock_page
            mock_fitz_open.return_value = mock_doc

            with patch("PIL.Image.open") as mock_image_open:
                mock_image = Mock()
                mock_image_open.return_value = mock_image

                with patch("io.BytesIO") as mock_bytesio:
                    mock_bytes_obj = Mock()
                    mock_bytes_obj.getvalue.return_value = b"processed_image_data"
                    mock_bytesio.return_value = mock_bytes_obj

                    result = ocr_processor.pdf_to_images(mock_pdf_content)

                    assert len(result) == 2
                    assert all(isinstance(img, bytes) for img in result)
                    mock_doc.close.assert_called_once()

    def test_pdf_to_images_exception_handling(self, ocr_processor):
        """Test that PDF document is properly closed on exception."""
        mock_pdf_content = b"invalid_pdf_content"

        with patch("fitz.open") as mock_fitz_open:
            mock_doc = Mock()
            mock_fitz_open.return_value = mock_doc
            mock_doc.load_page.side_effect = Exception("PDF processing error")
            mock_doc.__len__ = Mock(return_value=1)

            with pytest.raises(ValueError, match="Failed to convert PDF to images"):
                ocr_processor.pdf_to_images(mock_pdf_content)

            # Ensure document is closed even on exception
            mock_doc.close.assert_called_once()

    def test_get_unprocessed_files_incremental_mode(self, ocr_processor, mock_spark):
        """Test getting unprocessed files in incremental mode."""
        # Setup mock dataframes
        mock_source_df = Mock()
        mock_results_df = Mock()
        mock_unprocessed_df = Mock()

        mock_spark.table.side_effect = [mock_source_df, mock_results_df]
        mock_results_df.select.return_value.distinct.return_value.collect.return_value = [
            Mock(file_id="file1"),
            Mock(file_id="file2"),
        ]
        mock_source_df.filter.return_value = mock_unprocessed_df
        mock_unprocessed_df.limit.return_value.collect.return_value = ["file3", "file4"]

        result = ocr_processor.get_unprocessed_files()

        assert result == ["file3", "file4"]
        mock_source_df.filter.assert_called_once()

    def test_process_single_pdf_success(self, ocr_processor):
        """Test successful processing of a single PDF."""
        mock_file_row = Mock()
        mock_file_row.file_name = "test.pdf"
        mock_file_row.file_id = "test_file_id"
        mock_file_row.file_content = b"fake_pdf_content"

        # Mock pdf_to_images
        with patch.object(ocr_processor, "pdf_to_images") as mock_pdf_to_images:
            mock_pdf_to_images.return_value = [b"image1", b"image2"]

            # Mock Claude client response
            ocr_processor.claude_client.extract_text_from_image.return_value = {
                "extracted_text": "Sample text",
                "confidence_score": 0.95,
                "processing_duration_ms": 1500,
                "model": "claude-3-sonnet",
                "status": "success",
            }

            result = ocr_processor.process_single_pdf(mock_file_row)

            assert len(result) == 2  # Two pages
            assert all(r["file_id"] == "test_file_id" for r in result)
            assert all(r["extraction_status"] == "success" for r in result)

    def test_process_single_pdf_error_handling(self, ocr_processor):
        """Test error handling in single PDF processing."""
        mock_file_row = Mock()
        mock_file_row.file_name = "test.pdf"
        mock_file_row.file_id = "test_file_id"
        mock_file_row.file_content = b"invalid_pdf_content"

        # Mock pdf_to_images to raise exception
        with patch.object(ocr_processor, "pdf_to_images") as mock_pdf_to_images:
            mock_pdf_to_images.side_effect = ValueError("PDF processing failed")

            result = ocr_processor.process_single_pdf(mock_file_row)

            assert len(result) == 1
            assert result[0]["extraction_status"] == "failed"
            assert "PDF processing failed" in result[0]["error_message"]
