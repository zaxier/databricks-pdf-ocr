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
        from .config import create_spark_session
        spark = create_spark_session()
    
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


def run_ingestion_only():
    """Convenience function to run only PDF ingestion."""
    pipeline = create_pipeline()
    
    try:
        pipeline.setup_tables()
        pipeline.run_ingestion()
        print("PDF ingestion completed successfully")
    except Exception as e:
        print(f"PDF ingestion failed: {str(e)}")
        raise


def run_processing_only():
    """Convenience function to run only OCR processing."""
    pipeline = create_pipeline()
    
    try:
        result = pipeline.run_ocr_processing()
        print(f"OCR processing completed successfully: {result}")
    except Exception as e:
        print(f"OCR processing failed: {str(e)}")
        raise


def show_status():
    """Convenience function to show processing status and history."""
    pipeline = create_pipeline()
    
    try:
        # Show last run info
        last_run = pipeline.get_last_run_info()
        if last_run:
            print("=== Last Successful Run ===")
            print(f"Run ID: {last_run['run_id']}")
            print(f"Timestamp: {last_run['run_timestamp']}")
            print(f"Mode: {last_run['processing_mode']}")
            print(f"Files processed: {last_run['files_processed']}")
            print(f"Files succeeded: {last_run['files_succeeded']}")
            print(f"Files failed: {last_run['files_failed']}")
            print(f"Total pages: {last_run['total_pages_processed']}")
            print(f"Duration: {last_run['processing_duration_seconds']:.2f}s")
        else:
            print("No successful runs found")
        
        print("\n=== Recent Processing History ===")
        history = pipeline.get_processing_history(limit=5)
        if history:
            for run in history:
                print(f"{run['run_timestamp']} | {run['run_id']} | {run['processing_mode']} | "
                      f"Files: {run['files_processed']} | Success: {run['files_succeeded']} | "
                      f"Failed: {run['files_failed']}")
        else:
            print("No processing history found")
            
    except Exception as e:
        print(f"Failed to get status: {str(e)}")
        raise


if __name__ == "__main__":
    main()