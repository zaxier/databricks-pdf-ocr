import base64
import os
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from databricks_pdf_ocr.utils.claude_client import ClaudeClient


@pytest.fixture
def mock_workspace_client():
    """Mock WorkspaceClient for testing"""
    with patch("databricks_pdf_ocr.utils.claude_client.WorkspaceClient") as mock_wc:
        # Mock instance
        instance = Mock()
        mock_wc.return_value = instance
        
        # Mock config
        instance.config.host = "https://test.databricks.com"
        instance.config.token = "test-token"
        
        # Mock serving endpoints
        mock_endpoint = Mock()
        mock_endpoint.name = "databricks-claude-3-7-sonnet"
        instance.serving_endpoints.list.return_value = [mock_endpoint]
        
        # Mock get endpoint info
        endpoint_info = Mock()
        endpoint_info.name = "databricks-claude-3-7-sonnet"
        endpoint_info.creator = "test-user"
        endpoint_info.creation_timestamp = 1234567890
        endpoint_info.last_updated_timestamp = 1234567890
        endpoint_info.state = Mock(config_update="READY")
        instance.serving_endpoints.get.return_value = endpoint_info
        
        yield instance


@pytest.fixture
def claude_client(mock_workspace_client):
    """Create ClaudeClient instance for testing"""
    return ClaudeClient()


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables"""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", "test-token")


class TestClaudeClientInitialization:
    """Test ClaudeClient initialization and validation"""
    
    def test_init_with_defaults(self, mock_workspace_client):
        """Test initialization with default parameters"""
        client = ClaudeClient()
        
        assert client.endpoint_name == "databricks-claude-3-7-sonnet"
        assert client.max_tokens == 4096
        assert client.temperature == 0.0
        assert client.max_retries == 3
        assert client.retry_delay == 1.0
        assert client.timeout == 300.0
        assert client.workspace_client is not None
    
    def test_init_with_custom_params(self, mock_workspace_client):
        """Test initialization with custom parameters"""
        # Update mock to accept custom endpoint BEFORE creating client
        mock_endpoint = Mock()
        mock_endpoint.name = "custom-endpoint"
        mock_workspace_client.serving_endpoints.list.return_value = [mock_endpoint]
        
        client = ClaudeClient(
            endpoint_name="custom-endpoint",
            max_tokens=2048,
            temperature=0.5,
            max_retries=5,
            retry_delay=2.0,
            timeout=600.0
        )
        
        assert client.endpoint_name == "custom-endpoint"
        assert client.max_tokens == 2048
        assert client.temperature == 0.5
        assert client.max_retries == 5
        assert client.retry_delay == 2.0
        assert client.timeout == 600.0
    
    def test_validate_endpoint_not_found(self, mock_workspace_client):
        """Test validation fails when endpoint not found"""
        mock_workspace_client.serving_endpoints.list.return_value = []
        
        with pytest.raises(RuntimeError) as exc_info:
            ClaudeClient(endpoint_name="nonexistent-endpoint")
        
        assert "Failed to validate endpoint" in str(exc_info.value)
        assert "Endpoint 'nonexistent-endpoint' not found" in str(exc_info.value)
    
    def test_validate_endpoint_error(self, mock_workspace_client):
        """Test validation fails with runtime error"""
        mock_workspace_client.serving_endpoints.list.side_effect = Exception("API Error")
        
        with pytest.raises(RuntimeError) as exc_info:
            ClaudeClient()
        
        assert "Failed to validate endpoint" in str(exc_info.value)


class TestExtractTextFromImage:
    """Test extract_text_from_image method"""
    
    @patch("requests.post")
    def test_extract_text_success(self, mock_post, claude_client, mock_env_vars):
        """Test successful text extraction from image"""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{
                "message": {
                    "content": "Extracted text from image"
                }
            }],
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150
            }
        }
        mock_post.return_value = mock_response
        
        # Test image data
        image_data = b"test_image_data"
        
        result = claude_client.extract_text_from_image(image_data)
        
        # Verify result
        assert result["status"] == "success"
        assert result["extracted_text"] == "Extracted text from image"
        assert result["confidence_score"] > 0
        assert result["processing_time_ms"] >= 0
        assert result["model"] == "databricks-claude-3-7-sonnet"
        assert result["error"] is None
        assert result["token_usage"]["total_tokens"] == 150
        
        # Verify API call
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "serving-endpoints/databricks-claude-3-7-sonnet/invocations" in call_args[0][0]
        assert call_args[1]["headers"]["Authorization"] == "Bearer test-token"
    
    @patch("requests.post")
    def test_extract_text_with_custom_prompt(self, mock_post, claude_client, mock_env_vars):
        """Test extraction with custom prompt"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{
                "message": {"content": "Custom extraction"}
            }],
            "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
        }
        mock_post.return_value = mock_response
        
        custom_prompt = "Extract only numbers"
        result = claude_client.extract_text_from_image(b"image", custom_prompt=custom_prompt)
        
        # Verify custom prompt was used
        call_payload = mock_post.call_args[1]["json"]
        assert call_payload["messages"][0]["content"][1]["text"] == custom_prompt
    
    @patch("requests.post")
    def test_extract_text_api_error(self, mock_post, claude_client, mock_env_vars):
        """Test handling of API error response"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response
        
        result = claude_client.extract_text_from_image(b"image")
        
        assert result["status"] == "failed"
        assert result["extracted_text"] is None
        assert result["confidence_score"] == 0.0
        assert "API request failed with status 500" in result["error"]
    
    @patch("requests.post")
    def test_extract_text_with_retries(self, mock_post, claude_client, mock_env_vars):
        """Test retry logic on failure"""
        # First two attempts fail, third succeeds
        mock_post.side_effect = [
            Exception("Network error"),
            Exception("Timeout"),
            Mock(
                status_code=200,
                json=Mock(return_value={
                    "choices": [{"message": {"content": "Success after retries"}}],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
                })
            )
        ]
        
        result = claude_client.extract_text_from_image(b"image")
        
        assert result["status"] == "success"
        assert result["extracted_text"] == "Success after retries"
        assert mock_post.call_count == 3
    
    @patch("requests.post")
    def test_extract_text_all_retries_fail(self, mock_post, claude_client, mock_env_vars):
        """Test when all retry attempts fail"""
        mock_post.side_effect = Exception("Persistent error")
        
        result = claude_client.extract_text_from_image(b"image")
        
        assert result["status"] == "failed"
        assert result["extracted_text"] is None
        assert "Persistent error" in result["error"]
        assert mock_post.call_count == claude_client.max_retries
    
    @patch("requests.post")
    def test_extract_text_no_env_vars(self, mock_post, claude_client):
        """Test handling when environment variables are not set"""
        # Clear env vars
        if "DATABRICKS_HOST" in os.environ:
            del os.environ["DATABRICKS_HOST"]
        if "DATABRICKS_ACCESS_TOKEN" in os.environ:
            del os.environ["DATABRICKS_ACCESS_TOKEN"]
        
        # Mock the post request to avoid actual network call
        mock_post.side_effect = Exception("Network error")
        
        # Should use workspace client config
        result = claude_client.extract_text_from_image(b"image")
        
        # Should still attempt the request (will fail but that's ok for this test)
        assert result["status"] == "failed"


class TestExtractTextFromPdfPages:
    """Test extract_text_from_pdf_pages method"""
    
    @patch("requests.post")
    def test_extract_from_multiple_pages(self, mock_post, claude_client, mock_env_vars):
        """Test extraction from multiple PDF pages"""
        # Mock successful responses for each page
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Page text"}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
        }
        mock_post.return_value = mock_response
        
        page_images = [b"page1", b"page2", b"page3"]
        results = claude_client.extract_text_from_pdf_pages(page_images)
        
        assert len(results) == 3
        for i, result in enumerate(results):
            assert result["page_number"] == i + 1
            assert result["total_pages"] == 3
            assert result["status"] == "success"
            assert result["extracted_text"] == "Page text"
    
    @patch("requests.post")
    def test_extract_with_page_failures(self, mock_post, claude_client, mock_env_vars):
        """Test handling of individual page failures"""
        # Second page fails after all retries
        responses = []
        for i in range(claude_client.max_retries):
            responses.append(Exception("Page 2 error"))
        
        mock_post.side_effect = [
            Mock(
                status_code=200,
                json=Mock(return_value={
                    "choices": [{"message": {"content": "Page 1 text"}}],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
                })
            )
        ] + responses + [
            Mock(
                status_code=200,
                json=Mock(return_value={
                    "choices": [{"message": {"content": "Page 3 text"}}],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
                })
            )
        ]
        
        page_images = [b"page1", b"page2", b"page3"]
        results = claude_client.extract_text_from_pdf_pages(page_images)
        
        assert len(results) == 3
        assert results[0]["status"] == "success"
        assert results[1]["status"] == "failed"
        assert "Page 2 error" in results[1]["error"]
        assert results[2]["status"] == "success"


class TestCalculateConfidenceScore:
    """Test _calculate_confidence_score method"""
    
    def test_confidence_score_good_text(self, claude_client):
        """Test confidence score for good quality text"""
        text = "This is a clear and readable text with good quality."
        score = claude_client._calculate_confidence_score(text)
        
        assert score > 0.8
        assert score <= 1.0
    
    def test_confidence_score_with_unclear(self, claude_client):
        """Test confidence score with unclear markers"""
        text = "This text has [UNCLEAR] parts and another [UNCLEAR] section."
        score = claude_client._calculate_confidence_score(text)
        
        assert score < 0.9  # Reduced due to unclear markers
        assert score >= 0.5
    
    def test_confidence_score_short_text(self, claude_client):
        """Test confidence score for very short text"""
        text = "Short"
        score = claude_client._calculate_confidence_score(text)
        
        assert score < 0.8  # Reduced due to length
    
    def test_confidence_score_empty_text(self, claude_client):
        """Test confidence score for empty text"""
        score = claude_client._calculate_confidence_score("")
        assert score == 0.0
    
    def test_confidence_score_non_alpha(self, claude_client):
        """Test confidence score for text with few alphabetic characters"""
        text = "12345 67890 !@#$% ^&*() []{};"
        score = claude_client._calculate_confidence_score(text)
        
        assert score <= 0.8  # Reduced due to low alpha ratio


class TestTestConnection:
    """Test test_connection method"""
    
    @patch("requests.post")
    def test_connection_success(self, mock_post, claude_client, mock_env_vars):
        """Test successful connection test"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "TEST OK"}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
        }
        mock_post.return_value = mock_response
        
        result = claude_client.test_connection()
        
        assert result["status"] == "success"
        assert result["endpoint"] == "databricks-claude-3-7-sonnet"
        assert result["response_time_ms"] >= 0
        assert result["error"] is None
    
    @patch("requests.post")
    def test_connection_failure(self, mock_post, claude_client, mock_env_vars):
        """Test failed connection test"""
        mock_post.side_effect = Exception("Connection failed")
        
        result = claude_client.test_connection()
        
        assert result["status"] == "failed"
        assert result["endpoint"] == "databricks-claude-3-7-sonnet"
        assert result["response_time_ms"] == 0
        assert "Connection failed" in result["error"]


class TestGetEndpointInfo:
    """Test get_endpoint_info method"""
    
    def test_get_endpoint_info_success(self, claude_client, mock_workspace_client):
        """Test successful endpoint info retrieval"""
        result = claude_client.get_endpoint_info()
        
        assert result["name"] == "databricks-claude-3-7-sonnet"
        assert result["creator"] == "test-user"
        assert result["creation_timestamp"] == 1234567890
        assert result["state"] == "READY"
        assert result["config"]["max_tokens"] == 4096
        assert result["config"]["temperature"] == 0.0
    
    def test_get_endpoint_info_error(self, claude_client, mock_workspace_client):
        """Test endpoint info retrieval with error"""
        mock_workspace_client.serving_endpoints.get.side_effect = Exception("API Error")
        
        result = claude_client.get_endpoint_info()
        
        assert result["name"] == "databricks-claude-3-7-sonnet"
        assert "API Error" in result["error"]
        assert result["config"]["max_tokens"] == 4096


class TestCalculateEstimatedCost:
    """Test calculate_estimated_cost method"""
    
    def test_calculate_cost_with_tokens(self, claude_client):
        """Test cost calculation with token usage"""
        token_usage = {
            "prompt_tokens": 1000,
            "completion_tokens": 500,
            "total_tokens": 1500
        }
        
        result = claude_client.calculate_estimated_cost(token_usage)
        
        assert result["input_cost"] == 0.003  # 1000 * 0.003/1000
        assert result["output_cost"] == 0.0075  # 500 * 0.015/1000
        assert abs(result["total_cost"] - 0.0105) < 0.001  # Account for floating point precision
        assert result["currency"] == "USD"
        assert "Estimated costs" in result["note"]
    
    def test_calculate_cost_empty_usage(self, claude_client):
        """Test cost calculation with empty token usage"""
        result = claude_client.calculate_estimated_cost({})
        
        assert result["input_cost"] == 0.0
        assert result["output_cost"] == 0.0
        assert result["total_cost"] == 0.0


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    @patch("requests.post")
    def test_no_response_content(self, mock_post, claude_client, mock_env_vars):
        """Test handling when API returns no content"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"choices": []}
        mock_post.return_value = mock_response
        
        result = claude_client.extract_text_from_image(b"image")
        
        assert result["status"] == "failed"
        assert "No response content received" in result["error"]
    
    @patch("requests.post")
    def test_timeout_handling(self, mock_post, claude_client, mock_env_vars):
        """Test timeout handling"""
        mock_post.side_effect = requests.Timeout("Request timed out")
        
        result = claude_client.extract_text_from_image(b"image")
        
        assert result["status"] == "failed"
        assert "Request timed out" in result["error"]
    
    @patch("databricks_pdf_ocr.utils.claude_client.time.sleep")
    @patch("requests.post")
    def test_exponential_backoff(self, mock_post, mock_sleep, claude_client, mock_env_vars):
        """Test exponential backoff in retry logic"""
        mock_post.side_effect = Exception("Retry error")
        
        claude_client.extract_text_from_image(b"image")
        
        # Verify exponential backoff delays
        expected_delays = [
            claude_client.retry_delay,
            claude_client.retry_delay * 2,
        ]
        
        actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
        assert actual_delays == expected_delays