#!/usr/bin/env python3
"""
Databricks job entry point for PDF OCR processing
"""

import argparse
import sys
from typing import Dict, Any, List

from pyspark.sql import SparkSession

from ..config.settings import OCRConfig, ProcessingMode
from ..handlers.ocr_processor import OCRProcessor
from ..utils.spark import get_spark_session


def create_ocr_config(args: argparse.Namespace) -> OCRConfig:
    """Create OCR configuration from command line arguments"""
    
    # Parse processing mode
    processing_mode = ProcessingMode(args.mode)
    
    # Parse specific file IDs if provided
    specific_file_ids = []
    if args.specific_file_ids:
        specific_file_ids = [fid.strip() for fid in args.specific_file_ids.split(",")]
    
    return OCRConfig(
        catalog=args.catalog,
        schema=args.schema,
        target_table_path=args.target_table_path,
        state_table_path=args.state_table_path,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay_seconds,
        max_docs_per_run=args.max_docs_per_run,
        max_pages_per_pdf=args.max_pages_per_pdf,
        processing_mode=processing_mode,
        specific_file_ids=specific_file_ids,
        batch_size=args.batch_size,
        model_endpoint_name=args.model_endpoint_name,
        claude_max_tokens=args.claude_max_tokens,
        claude_temperature=args.claude_temperature,
        image_max_edge_pixels=args.image_max_edge_pixels,
        image_dpi=args.image_dpi
    )


def run_ocr_processing(spark: SparkSession, config: OCRConfig, command: str = "process") -> Dict[str, Any]:
    """Run the OCR processing job"""
    
    print(f"Starting PDF OCR processing - Command: {command}")
    print(f"Mode: {config.processing_mode}")
    print(f"Max docs per run: {config.max_docs_per_run}")
    print(f"Batch size: {config.batch_size}")
    print(f"Model endpoint: {config.model_endpoint_name}")
    
    # Create processor
    processor = OCRProcessor(spark, config)
    
    try:
        if command == "process":
            # Main processing command
            result = processor.process()
            
            return {
                "success": True,
                "command": command,
                "result": result.get_summary()
            }
            
        elif command == "stats":
            # Get processing statistics
            stats = processor.get_processing_stats()
            return {
                "success": True,
                "command": command,
                "stats": stats
            }
            
        elif command == "failed":
            # Get failed files
            failed_files = processor.get_failed_files(limit=100)
            failed_list = []
            
            for row in failed_files.collect():
                failed_list.append({
                    "file_id": row.file_id,
                    "file_path": row.file_path,
                    "file_name": row.file_name,
                    "attempts": row.processing_attempts,
                    "error": row.last_error
                })
            
            return {
                "success": True,
                "command": command,
                "failed_files": failed_list,
                "count": len(failed_list)
            }
            
        elif command == "reset-failed":
            # Reset failed files
            count = processor.reset_failed_files()
            return {
                "success": True,
                "command": command,
                "reset_count": count
            }
            
        elif command == "test-claude":
            # Test Claude API connection
            test_result = processor.test_claude_connection()
            return {
                "success": True,
                "command": command,
                "test_result": test_result
            }
            
        elif command == "process-specific":
            # Process specific files
            if not config.specific_file_ids:
                raise ValueError("No specific file IDs provided")
                
            result = processor.process_specific_files(config.specific_file_ids)
            return {
                "success": True,
                "command": command,
                "result": result.get_summary()
            }
            
        else:
            raise ValueError(f"Unknown command: {command}")
            
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def main():
    """Main entry point for the OCR processing job"""
    parser = argparse.ArgumentParser(
        description="Run PDF OCR processing job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Required arguments
    parser.add_argument(
        "--catalog",
        required=True,
        help="Databricks catalog name"
    )
    
    parser.add_argument(
        "--schema",
        required=True,
        help="Databricks schema name"
    )
    
    parser.add_argument(
        "--target-table-path",
        required=True,
        help="Path to target Delta table for OCR results"
    )
    
    parser.add_argument(
        "--state-table-path",
        required=True,
        help="Path to state Delta table for run tracking"
    )
    
    # Processing configuration
    parser.add_argument(
        "--mode",
        choices=["incremental", "reprocess_all", "reprocess_specific"],
        default="incremental",
        help="Processing mode"
    )
    
    parser.add_argument(
        "--max-docs-per-run",
        type=int,
        default=100,
        help="Maximum number of documents to process per run"
    )
    
    parser.add_argument(
        "--max-pages-per-pdf",
        type=int,
        default=None,
        help="Maximum number of pages to process per PDF"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of PDFs to process in parallel"
    )
    
    parser.add_argument(
        "--specific-file-ids",
        help="Comma-separated list of file IDs for reprocess_specific mode"
    )
    
    # Retry configuration
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum number of retries for failed operations"
    )
    
    parser.add_argument(
        "--retry-delay-seconds",
        type=int,
        default=60,
        help="Delay between retries in seconds"
    )
    
    # Claude API configuration
    parser.add_argument(
        "--model-endpoint-name",
        default="databricks-claude-3-7-sonnet",
        help="Name of the Claude model endpoint"
    )
    
    parser.add_argument(
        "--claude-max-tokens",
        type=int,
        default=4096,
        help="Maximum tokens for Claude API"
    )
    
    parser.add_argument(
        "--claude-temperature",
        type=float,
        default=0.0,
        help="Temperature for Claude API"
    )
    
    # Image processing configuration
    parser.add_argument(
        "--image-max-edge-pixels",
        type=int,
        default=1568,
        help="Maximum edge pixels for images"
    )
    
    parser.add_argument(
        "--image-dpi",
        type=int,
        default=200,
        help="DPI for PDF to image conversion"
    )
    
    # Command selection
    parser.add_argument(
        "--command",
        choices=["process", "stats", "failed", "reset-failed", "test-claude", "process-specific"],
        default="process",
        help="Command to execute"
    )
    
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration, don't run job"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without actually processing"
    )
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = get_spark_session("PDF_OCR_Processor")
    
    try:
        # Create configuration
        config = create_ocr_config(args)
        
        # Validate configuration
        config.validate()
        print("Configuration validation passed")
        
        if args.validate_only:
            print("Validation complete. Exiting.")
            sys.exit(0)
        
        # Create processor for dry run check
        if args.dry_run:
            processor = OCRProcessor(spark, config)
            
            # Show what would be processed
            from ..utils.state_manager import StateManager
            state_manager = StateManager(spark, config)
            
            pending_files = state_manager.get_pending_files(
                mode=config.processing_mode,
                max_files=config.max_docs_per_run,
                specific_ids=config.specific_file_ids
            )
            
            file_count = pending_files.count()
            print(f"DRY RUN: Would process {file_count} files")
            
            if file_count > 0:
                print("Files to process:")
                for row in pending_files.select("file_name", "processing_status", "processing_attempts").collect():
                    print(f"  - {row.file_name} (status: {row.processing_status}, attempts: {row.processing_attempts})")
            
            sys.exit(0)
        
        # Run the job
        result = run_ocr_processing(spark, config, args.command)
        
        if result["success"]:
            print("Job completed successfully")
            
            if "result" in result:
                summary = result["result"]
                print(f"Files processed: {summary.get('files_processed', 0)}")
                print(f"Files succeeded: {summary.get('files_succeeded', 0)}")
                print(f"Files failed: {summary.get('files_failed', 0)}")
                print(f"Pages processed: {summary.get('pages_processed', 0)}")
                print(f"Success rate: {summary.get('success_rate_percent', 0):.1f}%")
                print(f"Duration: {summary.get('duration_seconds', 0):.2f}s")
            
            if "stats" in result:
                print(f"Statistics: {result['stats']}")
            
            if "failed_files" in result:
                print(f"Failed files count: {result['count']}")
                for failed_file in result["failed_files"][:10]:  # Show first 10
                    print(f"  - {failed_file['file_name']} ({failed_file['attempts']} attempts): {failed_file['error']}")
            
            if "reset_count" in result:
                print(f"Reset {result['reset_count']} failed files")
            
            if "test_result" in result:
                test = result["test_result"]
                print(f"Claude API test: {test['status']} (response time: {test['response_time_ms']}ms)")
                if test.get("error"):
                    print(f"Error: {test['error']}")
        else:
            print(f"Job failed: {result['error']}")
            sys.exit(1)
            
    except Exception as e:
        print(f"FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()