"""OCR integration tests - simple test to verify markdown text extraction."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql import Row, SparkSession

from databricks_pdf_ocr.clients.claude import ClaudeClient
from databricks_pdf_ocr.config import ClaudeConfig, DatabricksConfig, OCRProcessingConfig
from databricks_pdf_ocr.processors.ocr import OCRProcessor


class TestOCRIntegration:
    """Simple integration test for OCR processor."""

    @pytest.fixture
    def ocr_processor(self, spark_session: SparkSession) -> OCRProcessor:
        """Create OCR processor for testing."""
        ocr_config = OCRProcessingConfig()
        claude_config = ClaudeConfig()
        databricks_config = DatabricksConfig()

        claude_client = ClaudeClient(claude_config, databricks_config)
        return OCRProcessor(spark_session, ocr_config, claude_client)

    @pytest.mark.integration
    def test_pdf_to_markdown_extraction(
        self,
        ocr_processor: OCRProcessor,
        test_pdf_files: dict[str, Path],
        integration_test_marker: bool,
    ) -> None:
        """Test that we can extract markdown formatted text from a PDF."""

        # Use the invoice PDF for testing
        invoice_path = test_pdf_files["invoice"]
        if not invoice_path.exists():
            pytest.skip("Invoice PDF not found")

        with open(invoice_path, "rb") as f:
            pdf_content = f.read()

        # Create mock file row
        mock_file_row = Row(
            file_id="test_invoice_123",
            file_name="test_invoice.pdf",
            file_content=pdf_content,
            upload_timestamp="2024-01-15T10:00:00Z",
        )

        # Mock Claude API to return markdown formatted text
        def mock_claude_extract_text(image_data: bytes) -> dict[str, Any]:
            return {
                "extracted_text": "# Invoice\n\n**Invoice Number:** INV-001\n\n**Date:** 2024-01-15\n\n## Items\n\n- Product A: $100.00\n- Product B: $50.00\n\n**Total:** $150.00",
                "confidence_score": 0.95,
                "status": "success",
                "model": "claude-3-sonnet",
                "processing_duration_ms": 1500,
            }

        with patch.object(
            ocr_processor.claude_client,
            "extract_text_from_image",
            side_effect=mock_claude_extract_text,
        ):
            # Process the PDF
            results = ocr_processor.process_single_pdf(mock_file_row)

            # Verify we got results
            assert len(results) > 0, "Should have at least one page result"

            # Check the first page result
            result = results[0]
            assert result["extraction_status"] == "success", "Extraction should succeed"
            assert result["extracted_text"] is not None, "Should have extracted text"

            # Verify the text is markdown formatted
            extracted_text = result["extracted_text"]
            assert "# Invoice" in extracted_text, "Should contain markdown header"
            assert "**" in extracted_text, "Should contain markdown bold formatting"
            assert "- " in extracted_text, "Should contain markdown list items"

            print(f"Successfully extracted markdown text: {extracted_text}")
