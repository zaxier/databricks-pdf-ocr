import base64
import time
import requests
import os
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole


class ClaudeClient:
    """Wrapper for Claude API via Databricks Model Serving"""

    def __init__(
        self,
        endpoint_name: str = "databricks-claude-3-7-sonnet",
        max_tokens: int = 4096,
        temperature: float = 0.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 300.0,
    ):
        self.endpoint_name = endpoint_name
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        
        # Initialize Databricks workspace client
        self.workspace_client = WorkspaceClient()
        
        # Test endpoint availability
        self._validate_endpoint()

    def _validate_endpoint(self) -> None:
        """Validate that the model serving endpoint is available"""
        try:
            endpoints = self.workspace_client.serving_endpoints.list()
            endpoint_names = [ep.name for ep in endpoints]
            
            if self.endpoint_name not in endpoint_names:
                raise ValueError(
                    f"Endpoint '{self.endpoint_name}' not found. Available endpoints: {endpoint_names}"
                )
                
        except Exception as e:
            raise RuntimeError(f"Failed to validate endpoint: {e}")

    def extract_text_from_image(
        self, 
        image_data: bytes, 
        image_format: str = "PNG",
        custom_prompt: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extract text from image using Claude API
        
        Args:
            image_data: Raw image bytes
            image_format: Image format (PNG, JPEG, etc.)
            custom_prompt: Optional custom prompt for extraction
            
        Returns:
            Dictionary containing extracted text and metadata
        """
        
        # Encode image to base64
        image_base64 = base64.b64encode(image_data).decode('utf-8')
        
        # Default OCR prompt
        if custom_prompt is None:
            custom_prompt = """Please extract all text from this image. 
            Maintain the original formatting and structure as much as possible.
            If there are tables, preserve the table structure.
            If there are multiple columns, clearly separate them.
            If you cannot read certain parts, indicate with [UNCLEAR].
            
            Only return the extracted text, no additional commentary."""
        
        # Get Databricks workspace URL and token for direct HTTP request
        workspace_url = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        if not workspace_url:
            # Try to get from workspace client config
            try:
                workspace_url = self.workspace_client.config.host.rstrip("/")
            except:
                raise ValueError("Could not determine Databricks workspace URL")
        
        token = os.getenv("DATABRICKS_ACCESS_TOKEN")
        if not token:
            # Try to get from workspace client config
            try:
                token = self.workspace_client.config.token
            except:
                raise ValueError("Could not determine Databricks access token")
        
        endpoint_url = f"{workspace_url}/serving-endpoints/{self.endpoint_name}/invocations"
        
        # Prepare headers and payload using direct HTTP like the working implementation
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Prepare the message content with actual image data
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/{image_format.lower()};base64,{image_base64}"}
                        },
                        {
                            "type": "text", 
                            "text": custom_prompt
                        }
                    ]
                }
            ],
            "max_tokens": self.max_tokens,
            "temperature": self.temperature
        }
        
        # Make the API call with retry logic
        for attempt in range(self.max_retries):
            try:
                start_time = time.time()
                
                response = requests.post(endpoint_url, headers=headers, json=payload, timeout=self.timeout)
                
                processing_time = time.time() - start_time
                
                if response.status_code == 200:
                    result = response.json()
                    # Extract text from Claude's response (Databricks format)
                    if "choices" in result and len(result["choices"]) > 0:
                        extracted_text = result["choices"][0]["message"]["content"]
                        
                        # Calculate confidence score (simplified approach)
                        confidence = self._calculate_confidence_score(extracted_text)
                        
                        return {
                            "extracted_text": extracted_text,
                            "confidence_score": confidence,
                            "processing_time_ms": int(processing_time * 1000),
                            "model": self.endpoint_name,
                            "status": "success",
                            "error": None,
                            "token_usage": {
                                "prompt_tokens": result.get("usage", {}).get("prompt_tokens", 0),
                                "completion_tokens": result.get("usage", {}).get("completion_tokens", 0),
                                "total_tokens": result.get("usage", {}).get("total_tokens", 0)
                            }
                        }
                    else:
                        raise ValueError("No response content received from Claude API")
                else:
                    raise ValueError(f"API request failed with status {response.status_code}: {response.text}")
                    
            except Exception as e:
                if attempt == self.max_retries - 1:
                    # Final attempt failed
                    return {
                        "extracted_text": None,
                        "confidence_score": 0.0,
                        "processing_time_ms": 0,
                        "model": self.endpoint_name,
                        "status": "failed",
                        "error": str(e),
                        "token_usage": {
                            "prompt_tokens": 0,
                            "completion_tokens": 0,
                            "total_tokens": 0
                        }
                    }
                else:
                    # Retry with exponential backoff
                    wait_time = self.retry_delay * (2 ** attempt)
                    time.sleep(wait_time)
                    continue
        
        # If we get here, all retries failed
        return {
            "extracted_text": None,
            "confidence_score": 0.0,
            "processing_time_ms": 0,
            "model": self.endpoint_name,
            "status": "failed",
            "error": "All retry attempts failed",
            "token_usage": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0
            }
        }

    def _calculate_confidence_score(self, text: str) -> float:
        """
        Calculate a confidence score for the extracted text
        This is a simplified implementation - in practice, you might use
        more sophisticated methods
        """
        if not text:
            return 0.0
            
        # Basic heuristics for confidence scoring
        score = 1.0
        
        # Check for unclear markers
        unclear_count = text.count("[UNCLEAR]")
        if unclear_count > 0:
            score -= min(unclear_count * 0.1, 0.5)
        
        # Check text length (very short text might be less reliable)
        if len(text.strip()) < 10:
            score -= 0.3
        
        # Check for reasonable character distribution
        if len(text) > 0:
            alpha_ratio = sum(c.isalpha() for c in text) / len(text)
            if alpha_ratio < 0.3:  # Too few alphabetic characters
                score -= 0.2
                
        return max(0.0, min(1.0, score))

    def extract_text_from_pdf_pages(
        self, 
        page_images: List[bytes], 
        image_format: str = "PNG"
    ) -> List[Dict[str, Any]]:
        """
        Extract text from multiple PDF pages
        
        Args:
            page_images: List of image bytes for each page
            image_format: Image format (PNG, JPEG, etc.)
            
        Returns:
            List of extraction results for each page
        """
        results = []
        
        for page_num, image_data in enumerate(page_images, 1):
            try:
                result = self.extract_text_from_image(image_data, image_format)
                result["page_number"] = page_num
                result["total_pages"] = len(page_images)
                results.append(result)
                
            except Exception as e:
                # Handle individual page failures
                results.append({
                    "page_number": page_num,
                    "total_pages": len(page_images),
                    "extracted_text": None,
                    "confidence_score": 0.0,
                    "processing_time_ms": 0,
                    "model": self.endpoint_name,
                    "status": "failed",
                    "error": str(e),
                    "token_usage": {
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0
                    }
                })
                
        return results

    def test_connection(self) -> Dict[str, Any]:
        """Test the connection to Claude API"""
        try:
            # Create a simple test image (1x1 white PNG)
            test_image = base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChAI9jU8A6wAAAABJRU5ErkJggg=="
            )
            
            result = self.extract_text_from_image(
                test_image, 
                "PNG", 
                "This is a test. Please respond with 'TEST OK'."
            )
            
            return {
                "status": "success" if result["status"] == "success" else "failed",
                "endpoint": self.endpoint_name,
                "response_time_ms": result["processing_time_ms"],
                "error": result.get("error")
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "endpoint": self.endpoint_name,
                "response_time_ms": 0,
                "error": str(e)
            }

    def get_endpoint_info(self) -> Dict[str, Any]:
        """Get information about the current endpoint"""
        try:
            endpoint = self.workspace_client.serving_endpoints.get(self.endpoint_name)
            return {
                "name": endpoint.name,
                "creator": endpoint.creator,
                "creation_timestamp": endpoint.creation_timestamp,
                "last_updated_timestamp": endpoint.last_updated_timestamp,
                "state": endpoint.state.config_update if endpoint.state else None,
                "config": {
                    "max_tokens": self.max_tokens,
                    "temperature": self.temperature,
                    "max_retries": self.max_retries,
                    "retry_delay": self.retry_delay,
                    "timeout": self.timeout
                }
            }
        except Exception as e:
            return {
                "name": self.endpoint_name,
                "error": str(e),
                "config": {
                    "max_tokens": self.max_tokens,
                    "temperature": self.temperature,
                    "max_retries": self.max_retries,
                    "retry_delay": self.retry_delay,
                    "timeout": self.timeout
                }
            }

    def calculate_estimated_cost(self, token_usage: Dict[str, int]) -> Dict[str, Any]:
        """
        Calculate estimated cost for API usage
        Note: This is a placeholder - actual pricing depends on your Databricks contract
        """
        # Placeholder pricing (update with actual rates)
        input_token_rate = 0.003 / 1000  # Per token
        output_token_rate = 0.015 / 1000  # Per token
        
        prompt_tokens = token_usage.get("prompt_tokens", 0)
        completion_tokens = token_usage.get("completion_tokens", 0)
        
        input_cost = prompt_tokens * input_token_rate
        output_cost = completion_tokens * output_token_rate
        total_cost = input_cost + output_cost
        
        return {
            "input_cost": input_cost,
            "output_cost": output_cost,
            "total_cost": total_cost,
            "currency": "USD",
            "note": "Estimated costs - actual pricing may vary"
        }