"""Tests for DatabricksClaudeOCR class."""

import io
from unittest.mock import MagicMock, Mock, patch

import pytest

from pdf_parse_claude import DatabricksClaudeOCR


class TestDatabricksClaudeOCR:
    """Test cases for DatabricksClaudeOCR class."""

    @pytest.fixture
    def ocr_instance(self):
        """Create a DatabricksClaudeOCR instance for testing."""
        return DatabricksClaudeOCR(
            workspace_url="https://test.databricks.com",
            token="test-token"
        )

    def test_initialization(self, ocr_instance):
        """Test proper initialization of DatabricksClaudeOCR."""
        assert ocr_instance.workspace_url == "https://test.databricks.com"
        assert ocr_instance.token == "test-token"
        assert ocr_instance.endpoint_name == "databricks-claude-3-7-sonnet"
        assert ocr_instance.endpoint_url == "https://test.databricks.com/serving-endpoints/databricks-claude-3-7-sonnet/invocations"

    def test_initialization_with_custom_endpoint(self):
        """Test initialization with custom endpoint name."""
        ocr = DatabricksClaudeOCR(
            workspace_url="https://test.databricks.com",
            token="test-token",
            endpoint_name="custom-claude-endpoint"
        )
        assert ocr.endpoint_name == "custom-claude-endpoint"
        assert ocr.endpoint_url == "https://test.databricks.com/serving-endpoints/custom-claude-endpoint/invocations"

    @patch('pdf_parse_claude.databricks_claude_ocr.fitz')
    def test_pdf_to_images(self, mock_fitz, ocr_instance):
        """Test PDF to images conversion."""
        # Mock the PDF document
        mock_page = MagicMock()
        mock_pixmap = MagicMock()
        mock_pixmap.tobytes.return_value = b"fake image data"
        mock_page.get_pixmap.return_value = mock_pixmap

        mock_doc = MagicMock()
        mock_doc.__len__.return_value = 2  # 2 pages
        mock_doc.__getitem__.return_value = mock_page
        mock_fitz.open.return_value = mock_doc
        mock_fitz.Matrix.return_value = MagicMock()

        # Test pdf_to_images
        images = ocr_instance.pdf_to_images("test.pdf")

        assert len(images) == 2
        assert images[0] == b"fake image data"
        assert images[1] == b"fake image data"
        mock_fitz.open.assert_called_once_with("test.pdf")
        mock_doc.close.assert_called_once()

    def test_extract_with_invalid_file(self, ocr_instance):
        """Test extraction with non-existent file."""
        with pytest.raises(FileNotFoundError):
            ocr_instance.extract_text_from_pdf("non_existent_file.pdf")

    @patch('pdf_parse_claude.databricks_claude_ocr.Image')
    @patch('pdf_parse_claude.databricks_claude_ocr.fitz')
    @patch('requests.post')
    def test_extract_text_from_pdf_success(
        self, mock_post, mock_fitz, mock_image, ocr_instance, tmp_path
    ):
        """Test successful text extraction from PDF."""
        # Create a test PDF
        test_pdf = tmp_path / "test.pdf"
        test_pdf.write_bytes(b"fake pdf content")

        # Mock PDF processing
        mock_page = MagicMock()
        mock_pixmap = MagicMock()
        mock_pixmap.tobytes.return_value = b"fake image data"
        mock_page.get_pixmap.return_value = mock_pixmap

        mock_doc = MagicMock()
        mock_doc.__len__.return_value = 1  # 1 page
        mock_doc.__getitem__.return_value = mock_page
        mock_fitz.open.return_value = mock_doc
        mock_fitz.Matrix.return_value = MagicMock()

        # Mock Image.open to bypass validation
        mock_img = MagicMock()
        mock_img.size = (100, 100)  # Small image that won't need resizing
        mock_image.open.return_value = mock_img

        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Extracted text from page"}}]
        }
        mock_post.return_value = mock_response

        # Test extraction
        result = ocr_instance.extract_text_from_pdf(str(test_pdf))

        assert "--- Page 1 ---" in result
        assert "Extracted text from page" in result
        mock_post.assert_called_once()
        mock_doc.close.assert_called_once()

    @patch('pdf_parse_claude.databricks_claude_ocr.Image')
    @patch('pdf_parse_claude.databricks_claude_ocr.fitz')
    @patch('requests.post')
    def test_extract_text_from_pdf_api_failure(
        self, mock_post, mock_fitz, mock_image, ocr_instance, tmp_path
    ):
        """Test text extraction with API failure."""
        # Create a test PDF
        test_pdf = tmp_path / "test.pdf"
        test_pdf.write_bytes(b"fake pdf content")

        # Mock PDF processing
        mock_page = MagicMock()
        mock_pixmap = MagicMock()
        mock_pixmap.tobytes.return_value = b"fake image data"
        mock_page.get_pixmap.return_value = mock_pixmap

        mock_doc = MagicMock()
        mock_doc.__len__.return_value = 1  # 1 page
        mock_doc.__getitem__.return_value = mock_page
        mock_fitz.open.return_value = mock_doc
        mock_fitz.Matrix.return_value = MagicMock()

        # Mock Image.open to bypass validation
        mock_img = MagicMock()
        mock_img.size = (100, 100)  # Small image that won't need resizing
        mock_image.open.return_value = mock_img

        # Mock failed API response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        # Test extraction - should handle error gracefully
        result = ocr_instance.extract_text_from_pdf(str(test_pdf))

        # Should return empty result due to error
        assert result == ""
        mock_post.assert_called_once()
        mock_doc.close.assert_called_once()

    @patch('pdf_parse_claude.databricks_claude_ocr.Image')
    def test_resize_image_if_needed_large_image(self, mock_image, ocr_instance):
        """Test image resizing when image is too large."""
        # Mock a large image that needs resizing
        mock_img = MagicMock()
        mock_img.size = (2000, 3000)  # Large image
        mock_image.open.return_value = mock_img
        mock_image.Resampling.LANCZOS = 1

        # Mock the resized image save
        mock_resized = MagicMock()
        mock_img.resize.return_value = mock_resized
        output_buffer = io.BytesIO()
        output_buffer.write(b"resized image data")
        output_buffer.seek(0)

        def mock_save(buffer, format):
            buffer.write(b"resized image data")

        mock_resized.save.side_effect = mock_save

        # Test resizing
        result = ocr_instance.resize_image_if_needed(b"original image data")

        # Check that resize was called with correct dimensions
        expected_ratio = 1568 / 3000  # max_edge / max(width, height)
        expected_width = int(2000 * expected_ratio)
        expected_height = int(3000 * expected_ratio)
        mock_img.resize.assert_called_once_with((expected_width, expected_height), 1)

        # Result should be the resized image data
        assert result == b"resized image data"

    @patch('pdf_parse_claude.databricks_claude_ocr.Image')
    def test_resize_image_if_needed_small_image(self, mock_image, ocr_instance):
        """Test image resizing when image is small enough."""
        # Mock a small image that doesn't need resizing
        mock_img = MagicMock()
        mock_img.size = (800, 600)  # Small image
        mock_image.open.return_value = mock_img

        # Test resizing
        original_data = b"original image data"
        result = ocr_instance.resize_image_if_needed(original_data)

        # Should not resize, return original data
        mock_img.resize.assert_not_called()
        assert result == original_data

    @patch('pdf_parse_claude.databricks_claude_ocr.Image')
    @patch('pdf_parse_claude.databricks_claude_ocr.fitz')
    @patch('requests.post')
    def test_extract_text_from_pdf_with_max_pages(
        self, mock_post, mock_fitz, mock_image, ocr_instance, tmp_path
    ):
        """Test text extraction with max_pages parameter."""
        # Create a test PDF
        test_pdf = tmp_path / "test.pdf"
        test_pdf.write_bytes(b"fake pdf content")

        # Mock PDF processing with 3 pages
        mock_page = MagicMock()
        mock_pixmap = MagicMock()
        mock_pixmap.tobytes.return_value = b"fake image data"
        mock_page.get_pixmap.return_value = mock_pixmap

        mock_doc = MagicMock()
        mock_doc.__len__.return_value = 3  # 3 pages
        mock_doc.__getitem__.return_value = mock_page
        mock_fitz.open.return_value = mock_doc
        mock_fitz.Matrix.return_value = MagicMock()

        # Mock Image.open to bypass validation
        mock_img = MagicMock()
        mock_img.size = (100, 100)
        mock_image.open.return_value = mock_img

        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Extracted text"}}]
        }
        mock_post.return_value = mock_response

        # Test extraction with max_pages=2
        result = ocr_instance.extract_text_from_pdf(str(test_pdf), max_pages=2)

        # Should only process 2 pages
        assert mock_post.call_count == 2
        assert "--- Page 1 ---" in result
        assert "--- Page 2 ---" in result
        assert "--- Page 3 ---" not in result
        mock_doc.close.assert_called_once()

    @patch('pdf_parse_claude.databricks_claude_ocr.Image')
    @patch('pdf_parse_claude.databricks_claude_ocr.fitz')
    @patch('requests.post')
    def test_extract_text_from_pdf_no_choices(
        self, mock_post, mock_fitz, mock_image, ocr_instance, tmp_path
    ):
        """Test text extraction when API returns no choices."""
        # Create a test PDF
        test_pdf = tmp_path / "test.pdf"
        test_pdf.write_bytes(b"fake pdf content")

        # Mock PDF processing
        mock_page = MagicMock()
        mock_pixmap = MagicMock()
        mock_pixmap.tobytes.return_value = b"fake image data"
        mock_page.get_pixmap.return_value = mock_pixmap

        mock_doc = MagicMock()
        mock_doc.__len__.return_value = 1
        mock_doc.__getitem__.return_value = mock_page
        mock_fitz.open.return_value = mock_doc
        mock_fitz.Matrix.return_value = MagicMock()

        # Mock Image.open to bypass validation
        mock_img = MagicMock()
        mock_img.size = (100, 100)
        mock_image.open.return_value = mock_img

        # Mock API response with no choices
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"choices": []}
        mock_post.return_value = mock_response

        # Test extraction
        result = ocr_instance.extract_text_from_pdf(str(test_pdf))

        # Should handle missing choices gracefully
        assert result == ""
        mock_post.assert_called_once()
        mock_doc.close.assert_called_once()

