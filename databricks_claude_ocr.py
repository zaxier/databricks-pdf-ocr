#!/usr/bin/env python3

import base64
import requests
import os
from typing import Optional
import fitz  # PyMuPDF
from PIL import Image
import io

class DatabricksClaudeOCR:
    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.endpoint_url = f"{self.workspace_url}/serving-endpoints/databricks-claude-3-7-sonnet/invocations"
        
    def pdf_to_images(self, pdf_path: str, dpi: int = 200) -> list[bytes]:
        """Convert PDF pages to images"""
        images = []
        pdf_document = fitz.open(pdf_path)
        
        for page_num in range(len(pdf_document)):
            page = pdf_document[page_num]
            mat = fitz.Matrix(dpi/72.0, dpi/72.0)
            pix = page.get_pixmap(matrix=mat)
            img_data = pix.tobytes("png")
            images.append(img_data)
            
        pdf_document.close()
        return images
    
    def resize_image_if_needed(self, image_data: bytes, max_edge: int = 1568) -> bytes:
        """Resize image if it exceeds Claude's recommended size"""
        img = Image.open(io.BytesIO(image_data))
        width, height = img.size
        
        if max(width, height) > max_edge:
            ratio = max_edge / max(width, height)
            new_width = int(width * ratio)
            new_height = int(height * ratio)
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            
            output = io.BytesIO()
            img.save(output, format='PNG')
            return output.getvalue()
        
        return image_data
    
    def extract_text_from_pdf(self, pdf_path: str, max_pages: Optional[int] = None) -> str:
        """Extract text from PDF using Claude's vision capabilities"""
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
            
        print(f"Converting PDF to images...")
        images = self.pdf_to_images(pdf_path)
        
        if max_pages:
            images = images[:max_pages]
            
        all_text = []
        
        for i, image_data in enumerate(images):
            print(f"Processing page {i+1}/{len(images)}...")
            
            # Resize if needed
            image_data = self.resize_image_if_needed(image_data)
            
            # Convert to base64
            base64_image = base64.b64encode(image_data).decode('utf-8')
            
            # Prepare the request
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{base64_image}"
                                }
                            },
                            {
                                "type": "text",
                                "text": "Please extract all text from this image. Preserve the original formatting and structure as much as possible. If there are tables, maintain their structure. Return only the extracted text without any additional commentary."
                            }
                        ]
                    }
                ],
                "max_tokens": 4096,
                "temperature": 0
            }
            
            # Make the request
            response = requests.post(self.endpoint_url, headers=headers, json=payload)
            
            if response.status_code == 200:
                result = response.json()
                # Extract text from Claude's response (Databricks format)
                if 'choices' in result and len(result['choices']) > 0:
                    page_text = result['choices'][0]['message']['content']
                    all_text.append(f"--- Page {i+1} ---\n{page_text}\n")
                else:
                    print(f"Warning: No text extracted from page {i+1}")
            else:
                print(f"Error on page {i+1}: {response.status_code} - {response.text}")
        
        return "\n".join(all_text)

def main():
    # Configuration
    WORKSPACE_URL = os.getenv('DATABRICKS_HOST', 'https://your-workspace.databricks.net')
    DATABRICKS_TOKEN = os.getenv('DATABRICKS_ACCESS_TOKEN', 'your-databricks-token')
    
    # Initialize the OCR client
    ocr_client = DatabricksClaudeOCR(WORKSPACE_URL, DATABRICKS_TOKEN)
    
    # Path to the PDF
    pdf_path = "data/example.pdf"
    
    try:
        print(f"Starting OCR extraction for: {pdf_path}")
        extracted_text = ocr_client.extract_text_from_pdf(pdf_path)
        
        # Save the extracted text
        output_path = "data/extracted_text.txt"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(extracted_text)
            
        print(f"\nExtraction complete! Text saved to: {output_path}")
        print(f"\nFirst 500 characters of extracted text:")
        print("-" * 50)
        print(extracted_text[:500])
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
