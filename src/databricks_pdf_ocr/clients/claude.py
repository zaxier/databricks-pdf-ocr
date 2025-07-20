"""Claude client for OCR processing via Databricks model serving."""

import base64
import io
import time
from typing import Any

import mlflow.deployments
from PIL import Image

from ..config import ClaudeConfig, DatabricksConfig


class ClaudeClient:
    """Client for Claude API via Databricks model serving."""

    def __init__(self, config: ClaudeConfig, databricks_config: DatabricksConfig):
        self.config = config
        self.databricks_config = databricks_config
        self.client = mlflow.deployments.get_deploy_client("databricks")

    def resize_image_if_needed(self, image_data: bytes) -> bytes:
        """Resize image if it exceeds Claude's recommended size."""
        img = Image.open(io.BytesIO(image_data))
        width, height = img.size

        max_edge = self.config.image_max_edge_pixels
        if max(width, height) > max_edge:
            ratio = max_edge / max(width, height)
            new_width = int(width * ratio)
            new_height = int(height * ratio)
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

            output = io.BytesIO()
            img.save(output, format="PNG")
            return output.getvalue()

        return image_data

    def extract_text_from_image(self, image_data: bytes) -> dict[str, Any]:
        """Extract text from image using Claude API."""
        try:
            start_time = time.time()

            # Resize if needed
            image_data = self.resize_image_if_needed(image_data)

            # Convert to base64
            base64_image = base64.b64encode(image_data).decode("utf-8")

            # Prepare the request
            inputs = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{base64_image}"}
                            },
                            {
                                "type": "text",
                                "text": "Please extract all text from this image. Preserve the original formatting and structure as much as possible. If there are tables, maintain their structure. Return only the extracted text without any additional commentary."
                            }
                        ]
                    }
                ],
                "max_tokens": self.config.max_tokens,
                "temperature": self.config.temperature
            }

            # Make the request
            if not self.client:
                raise RuntimeError("MLflow deployment client is not initialized")

            response = self.client.predict(
                endpoint=self.config.endpoint_name,
                inputs=inputs
            )

            processing_time = int((time.time() - start_time) * 1000)

            # Extract text from response
            if response and "choices" in response and len(response["choices"]) > 0:
                extracted_text = response["choices"][0]["message"]["content"]
                return {
                    "extracted_text": extracted_text,
                    "confidence_score": 0.95,  # Claude doesn't provide confidence, using default
                    "status": "success",
                    "processing_duration_ms": processing_time,
                    "model": self.config.endpoint_name
                }
            else:
                return {
                    "extracted_text": None,
                    "confidence_score": None,
                    "status": "failed",
                    "error": "No content in Claude response",
                    "processing_duration_ms": processing_time,
                    "model": self.config.endpoint_name
                }

        except Exception as e:
            return {
                "extracted_text": None,
                "confidence_score": None,
                "status": "failed",
                "error": str(e),
                "processing_duration_ms": 0,
                "model": self.config.endpoint_name
            }
