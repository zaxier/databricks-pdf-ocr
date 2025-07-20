"""
Core PDF Processing Components - Simplified Version

This file contains the essential logic for:
1. PDF ingestion using Databricks Autoloader 
2. OCR processing using Claude AI API

Extracted from the full production pipeline for a clean restart.
"""

import io
import time
from datetime import datetime
from typing import Any, Optional

import fitz  # PyMuPDF
from PIL import Image
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, sha2, regexp_extract


class SimplePDFIngester:
    """Core PDF ingestion using Databricks Autoloader"""
    
    def __init__(self, spark: SparkSession, source_path: str, target_table: str, checkpoint_path: str):
        self.spark = spark
        self.source_path = source_path
        self.target_table = target_table
        self.checkpoint_path = checkpoint_path
    
    def ingest_pdfs(self) -> None:
        """Ingest PDFs from volume to Delta table"""
        print(f"Starting PDF ingestion from: {self.source_path}")
        
        # Configure autoloader for PDF files
        autoloader_options = {
            "cloudFiles.format": "binaryFile",
            "cloudFiles.includeExistingFiles": "true",
            "cloudFiles.maxFilesPerTrigger": "100",
            "cloudFiles.schemaHints": "path STRING, modificationTime TIMESTAMP, length LONG, content BINARY"
        }
        
        # Read PDF files using autoloader
        df = (
            self.spark.readStream
            .format("cloudFiles")
            .options(**autoloader_options)
            .load(self.source_path)
        )
        
        # Filter for PDF files only
        pdf_df = df.filter(col("path").rlike(r".*\.(pdf|PDF)$"))
        
        # Transform to target schema
        transformed_df = pdf_df.select(
            sha2(col("path"), 256).alias("file_id"),
            col("path").alias("file_path"),
            regexp_extract(col("path"), r"([^/]+)$", 1).alias("file_name"),
            col("length").alias("file_size"),
            col("content").alias("file_content"),
            col("modificationTime").alias("modification_time"),
            current_timestamp().alias("ingestion_timestamp"),
            lit("pending").alias("processing_status")
        )
        
        # Write to Delta table
        query = (
            transformed_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.target_table)
        )
        
        # Wait for completion
        query.awaitTermination()
        print("PDF ingestion completed")


class SimpleClaudeClient:
    """Simplified Claude API client for OCR"""
    
    def __init__(self, endpoint_name: str, max_tokens: int = 4000):
        self.endpoint_name = endpoint_name
        self.max_tokens = max_tokens
    
    def extract_text_from_image(self, image_data: bytes) -> dict[str, Any]:
        """Extract text from image using Claude API"""
        try:
            # This would connect to your Databricks Model Serving endpoint
            # For now, returning mock structure
            print(f"Calling Claude endpoint: {self.endpoint_name}")
            
            # Mock API call - replace with actual endpoint call
            extracted_text = "Sample extracted text from image"
            confidence_score = 0.95
            
            return {
                "extracted_text": extracted_text,
                "confidence_score": confidence_score,
                "status": "success"
            }
            
        except Exception as e:
            return {
                "extracted_text": None,
                "confidence_score": None,
                "status": "failed",
                "error": str(e)
            }


class SimplePDFProcessor:
    """Core OCR processing logic"""
    
    def __init__(self, spark: SparkSession, source_table: str, results_table: str, 
                 claude_client: SimpleClaudeClient):
        self.spark = spark
        self.source_table = source_table
        self.results_table = results_table
        self.claude_client = claude_client
    
    def process_pending_pdfs(self, max_files: int = 10) -> None:
        """Process pending PDFs for OCR"""
        print(f"Processing up to {max_files} pending PDFs")
        
        # Get pending files
        pending_files = (
            self.spark.table(self.source_table)
            .filter(col("processing_status") == "pending")
            .limit(max_files)
            .collect()
        )
        
        if not pending_files:
            print("No pending files to process")
            return
        
        print(f"Found {len(pending_files)} files to process")
        
        # Process each file
        for file_row in pending_files:
            try:
                self._process_single_pdf(file_row)
            except Exception as e:
                print(f"Error processing {file_row.file_name}: {str(e)}")
                self._update_file_status(file_row.file_id, "failed", str(e))
    
    def _process_single_pdf(self, file_row) -> None:
        """Process a single PDF file"""
        print(f"Processing: {file_row.file_name}")
        
        # Update status to processing
        self._update_file_status(file_row.file_id, "processing")
        
        try:
            # Convert PDF to images
            images = self._pdf_to_images(file_row.file_content)
            
            if not images:
                raise ValueError("No images extracted from PDF")
            
            # Process each page
            page_results = []
            for page_num, image_data in enumerate(images, 1):
                print(f"  Processing page {page_num}/{len(images)}")
                
                # Extract text using Claude
                start_time = time.time()
                ocr_result = self.claude_client.extract_text_from_image(image_data)
                processing_time = int((time.time() - start_time) * 1000)
                
                # Store page result
                page_result = {
                    "file_id": file_row.file_id,
                    "page_number": page_num,
                    "total_pages": len(images),
                    "extracted_text": ocr_result.get("extracted_text"),
                    "extraction_confidence": ocr_result.get("confidence_score"),
                    "processing_timestamp": datetime.now(),
                    "processing_duration_ms": processing_time,
                    "extraction_status": ocr_result.get("status", "failed"),
                    "error_message": ocr_result.get("error")
                }
                page_results.append(page_result)
            
            # Store all page results
            if page_results:
                results_df = self.spark.createDataFrame(page_results)
                results_df.write.mode("append").saveAsTable(self.results_table)
                
                # Update file status to completed
                self._update_file_status(file_row.file_id, "completed")
                print(f"  Completed: {file_row.file_name}")
            else:
                raise ValueError("No page results generated")
        
        except Exception as e:
            error_msg = f"Failed to process PDF: {str(e)}"
            self._update_file_status(file_row.file_id, "failed", error_msg)
            raise
    
    def _pdf_to_images(self, pdf_content: bytes, dpi: int = 150) -> list[bytes]:
        """Convert PDF to list of PNG images"""
        images = []
        
        try:
            # Open PDF from bytes
            doc = fitz.open(stream=pdf_content, filetype="pdf")
            
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                
                # Convert to image
                mat = fitz.Matrix(dpi / 72, dpi / 72)
                pix = page.get_pixmap(matrix=mat)
                
                # Convert to PIL Image and then to bytes
                img_data = pix.tobytes("png")
                img = Image.open(io.BytesIO(img_data))
                
                # Convert back to bytes
                img_bytes = io.BytesIO()
                img.save(img_bytes, format="PNG")
                images.append(img_bytes.getvalue())
            
            doc.close()
            
        except Exception as e:
            raise ValueError(f"Failed to convert PDF to images: {str(e)}")
        
        return images
    
    def _update_file_status(self, file_id: str, status: str, error_message: Optional[str] = None) -> None:
        """Update file processing status"""
        update_sql = f"""
        UPDATE {self.source_table}
        SET processing_status = '{status}',
            last_error = {'NULL' if error_message is None else f"'{error_message}'"}
        WHERE file_id = '{file_id}'
        """
        self.spark.sql(update_sql)


# Example usage:
def run_pdf_pipeline(spark: SparkSession, catalog: str, schema: str, volume_path: str):
    """Complete PDF processing pipeline"""
    
    # Table names
    source_table = f"{catalog}.{schema}.pdf_source"
    results_table = f"{catalog}.{schema}.pdf_ocr_results"
    checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints"
    
    # Step 1: Ingest PDFs
    ingester = SimplePDFIngester(
        spark=spark,
        source_path=volume_path,
        target_table=source_table,
        checkpoint_path=checkpoint_path
    )
    ingester.ingest_pdfs()
    
    # Step 2: Process PDFs with OCR
    claude_client = SimpleClaudeClient(endpoint_name="databricks-claude-3-7-sonnet")
    processor = SimplePDFProcessor(
        spark=spark,
        source_table=source_table,
        results_table=results_table,
        claude_client=claude_client
    )
    processor.process_pending_pdfs(max_files=10)
    
    print("Pipeline completed!")


# Schema creation helpers
def create_source_table_sql(catalog: str, schema: str) -> str:
    """SQL to create source table"""
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pdf_source (
        file_id STRING NOT NULL,
        file_path STRING NOT NULL,
        file_name STRING NOT NULL,
        file_size BIGINT,
        file_content BINARY,
        modification_time TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        processing_status STRING DEFAULT 'pending',
        last_error STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name'
    )
    """

def create_results_table_sql(catalog: str, schema: str) -> str:
    """SQL to create results table"""
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pdf_ocr_results (
        file_id STRING NOT NULL,
        page_number INT NOT NULL,
        total_pages INT NOT NULL,
        extracted_text STRING,
        extraction_confidence DOUBLE,
        processing_timestamp TIMESTAMP,
        processing_duration_ms BIGINT,
        extraction_status STRING,
        error_message STRING
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name'
    )
    """