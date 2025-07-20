"""Main module for PDF OCR pipeline."""

import time
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from .config import AutoloaderConfig, OCRProcessingConfig, ClaudeConfig, DatabricksConfig
from .handlers.autoloader import AutoloaderHandler
from .clients.claude import ClaudeClient
from .processors.ocr import OCRProcessor
from .managers.state import StateManager
from .schemas import create_source_table_sql, create_target_table_sql, create_state_table_sql


class PDFOCRPipeline:
    """Main pipeline for PDF OCR processing."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
        # Initialize configurations
        self.autoloader_config = AutoloaderConfig()
        self.ocr_config = OCRProcessingConfig()
        self.claude_config = ClaudeConfig()
        self.databricks_config = DatabricksConfig()
        
        # Initialize components
        self.autoloader_handler = AutoloaderHandler(spark, self.autoloader_config)
        self.claude_client = ClaudeClient(self.claude_config, self.databricks_config)
        self.ocr_processor = OCRProcessor(spark, self.ocr_config, self.claude_client)
        self.state_manager = StateManager(spark, self.ocr_config)
    
    def setup_tables(self) -> None:
        """Create required tables if they don't exist."""
        print("Setting up database tables...")
        
        # Create source table
        self.spark.sql(create_source_table_sql(self.autoloader_config.source_table_path))
        print(f"Source table ready: {self.autoloader_config.source_table_path}")
        
        # Create target table
        self.spark.sql(create_target_table_sql(self.ocr_config.target_table_path))
        print(f"Target table ready: {self.ocr_config.target_table_path}")
        
        # Create state table
        self.spark.sql(create_state_table_sql(self.ocr_config.state_table_path))
        print(f"State table ready: {self.ocr_config.state_table_path}")
    
    def run_ingestion(self) -> None:
        """Run PDF ingestion from volume to Delta table."""
        print("=== Starting PDF Ingestion ===")
        self.autoloader_handler.ingest_pdfs_batch()
        print("=== PDF Ingestion Complete ===\n")
    
    def run_ocr_processing(self) -> dict:
        """Run OCR processing on unprocessed PDFs."""
        print("=== Starting OCR Processing ===")
        
        start_time = time.time()
        
        # Create run record
        run_config = {
            "processing_mode": self.ocr_config.processing_mode,
            "max_docs_per_run": self.ocr_config.max_docs_per_run,
            "max_pages_per_pdf": self.ocr_config.max_pages_per_pdf,
            "batch_size": self.ocr_config.batch_size,
            "claude_endpoint": self.claude_config.endpoint_name,
            "max_tokens": self.claude_config.max_tokens,
            "timestamp": datetime.now().isoformat()
        }
        
        run_id = self.state_manager.create_run_record(run_config)
        print(f"Started run: {run_id}")
        
        try:
            # Process batch
            stats = self.ocr_processor.process_batch()
            
            # Update run record
            duration_seconds = time.time() - start_time
            self.state_manager.update_run_record(run_id, stats, duration_seconds)
            
            print(f"=== OCR Processing Complete (Run: {run_id}) ===")
            print(f"Duration: {duration_seconds:.2f}s")
            print(f"Stats: {stats}\n")
            
            return {"run_id": run_id, "stats": stats, "duration_seconds": duration_seconds}
            
        except Exception as e:
            print(f"Error in OCR processing: {str(e)}")
            # Update run record with error
            duration_seconds = time.time() - start_time
            error_stats = {
                "files_processed": 0,
                "files_succeeded": 0,
                "files_failed": 0,
                "total_pages_processed": 0
            }
            self.state_manager.update_run_record(run_id, error_stats, duration_seconds)
            raise
    
    def run_full_pipeline(self) -> dict:
        """Run the complete pipeline: ingestion + OCR processing."""
        print("=== Starting Full PDF OCR Pipeline ===")
        
        # Setup tables
        self.setup_tables()
        
        # Run ingestion
        self.run_ingestion()
        
        # Run OCR processing
        result = self.run_ocr_processing()
        
        print("=== Full Pipeline Complete ===")
        return result
    
    def get_processing_history(self, limit: int = 10) -> list:
        """Get processing history."""
        return self.state_manager.get_processing_history(limit)
    
    def get_last_run_info(self) -> dict:
        """Get information about the last successful run."""
        return self.state_manager.get_last_successful_run()


def create_pipeline(spark: Optional[SparkSession] = None) -> PDFOCRPipeline:
    """Create a PDFOCRPipeline instance."""
    if spark is None:
        # Import spark creation utility if available
        try:
            from lightning.spark import get_spark_session
            spark = get_spark_session()
        except ImportError:
            # Fallback to Databricks Connect session
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.getOrCreate()
    
    return PDFOCRPipeline(spark)


def main():
    """Main entry point for running the pipeline."""
    pipeline = create_pipeline()
    
    try:
        result = pipeline.run_full_pipeline()
        print(f"Pipeline completed successfully: {result}")
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()