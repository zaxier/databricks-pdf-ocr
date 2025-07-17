#!/usr/bin/env python3
"""
Main CLI entry point for the Databricks PDF OCR Pipeline

This provides a unified interface for running both autoloader ingestion
and OCR processing tasks.
"""

import argparse
import logging
import sys

from .config.settings import AutoloaderConfig, OCRConfig, ProcessingMode
from .handlers.autoloader import AutoloaderHandler
from .handlers.ocr_processor import OCRProcessor
from .utils.spark import get_spark_session


def setup_logging(log_level: str) -> logging.Logger:
    """Set up logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(__name__)


def run_autoloader_mode(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Run autoloader ingestion mode"""
    logger.info("Running autoloader ingestion")

    # Validate required arguments
    required_args = ["catalog", "schema", "source_volume_path", "checkpoint_location"]
    missing_args = [arg for arg in required_args if not getattr(args, arg, None)]

    if missing_args:
        logger.error(f"Missing required arguments for autoloader mode: {missing_args}")
        return 1

    # Create configuration
    config = AutoloaderConfig(
        catalog=args.catalog,
        schema=args.schema,
        source_volume_path=args.source_volume_path,
        checkpoint_location=args.checkpoint_location,
        source_table_path=f"{args.catalog}.{args.schema}.pdf_source",
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay_seconds,
    )

    # Create Spark session
    spark = get_spark_session("PDF_Autoloader_CLI")

    try:
        # Create handler
        handler = AutoloaderHandler(spark, config)

        # Validate configuration
        validation_result = handler.validate_volume_path()
        if not validation_result["valid"]:
            logger.error(f"Volume path validation failed: {validation_result['error']}")
            return 1

        logger.info(f"Found {validation_result['pdf_files']} PDF files in volume")

        # Run based on sub-command
        if args.autoloader_command == "stream":
            # Start streaming
            result = handler.process()
            logger.info(f"Streaming started: {result.get_summary()}")

            # Wait for termination
            if hasattr(handler, "streaming_query") and handler.streaming_query:
                logger.info("Streaming job running. Press Ctrl+C to stop.")
                try:
                    handler.wait_for_termination()
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, stopping...")
                    handler.stop_stream()

        elif args.autoloader_command == "rescan":
            # One-time rescan
            result = handler.rescan_volume()
            logger.info(f"Rescan completed: {result.get_summary()}")

        elif args.autoloader_command == "stats":
            # Get statistics
            stats = handler.get_ingestion_stats()
            logger.info(f"Ingestion statistics: {stats}")

        return 0

    except Exception as e:
        logger.error(f"Error in autoloader mode: {e}", exc_info=True)
        return 1
    finally:
        spark.stop()


def run_ocr_mode(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Run OCR processing mode"""
    logger.info("Running OCR processing")

    # Validate required arguments
    required_args = ["catalog", "schema"]
    missing_args = [arg for arg in required_args if not getattr(args, arg, None)]

    if missing_args:
        logger.error(f"Missing required arguments for OCR mode: {missing_args}")
        return 1

    # Parse processing mode
    processing_mode = ProcessingMode(args.processing_mode)

    # Parse specific file IDs if provided
    specific_file_ids = []
    if args.specific_file_ids:
        specific_file_ids = [fid.strip() for fid in args.specific_file_ids.split(",")]

    # Create configuration
    config = OCRConfig(
        catalog=args.catalog,
        schema=args.schema,
        target_table_path=f"{args.catalog}.{args.schema}.pdf_ocr_results",
        state_table_path=f"{args.catalog}.{args.schema}.pdf_processing_state",
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
        image_dpi=args.image_dpi,
    )

    # Create Spark session
    spark = get_spark_session("PDF_OCR_CLI")

    try:
        # Validate configuration
        config.validate()

        # Create processor
        processor = OCRProcessor(spark, config)

        # Run based on sub-command
        if args.ocr_command == "process":
            # Main processing
            result = processor.process()
            summary = result.get_summary()
            logger.info(f"OCR processing completed: {summary}")

            # Print detailed results
            print(f"Files processed: {summary['files_processed']}")
            print(f"Files succeeded: {summary['files_succeeded']}")
            print(f"Files failed: {summary['files_failed']}")
            print(f"Pages processed: {summary['pages_processed']}")
            print(f"Success rate: {summary['success_rate_percent']:.1f}%")
            print(f"Duration: {summary['duration_seconds']:.2f}s")

        elif args.ocr_command == "stats":
            # Get statistics
            stats = processor.get_processing_stats()
            logger.info(f"Processing statistics: {stats}")
            print(f"Statistics: {stats}")

        elif args.ocr_command == "failed":
            # Get failed files
            failed_files = processor.get_failed_files(limit=50)
            failed_list = failed_files.collect()

            logger.info(f"Found {len(failed_list)} failed files")
            print(f"Failed files ({len(failed_list)}):")
            for row in failed_list:
                print(f"  - {row.file_name} ({row.processing_attempts} attempts): {row.last_error}")

        elif args.ocr_command == "reset-failed":
            # Reset failed files
            count = processor.reset_failed_files()
            logger.info(f"Reset {count} failed files")
            print(f"Reset {count} failed files to pending status")

        elif args.ocr_command == "test-claude":
            # Test Claude API
            test_result = processor.test_claude_connection()
            logger.info(f"Claude API test result: {test_result}")
            print(f"Claude API test: {test_result['status']}")
            if test_result.get("error"):
                print(f"Error: {test_result['error']}")

        return 0

    except Exception as e:
        logger.error(f"Error in OCR mode: {e}", exc_info=True)
        return 1
    finally:
        spark.stop()


def main() -> int:
    """Main entry point for the databricks-pdf-ocr application."""
    parser = argparse.ArgumentParser(
        description="Databricks PDF OCR Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Global arguments
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    parser.add_argument("--catalog", required=True, help="Databricks catalog name")

    parser.add_argument("--schema", required=True, help="Databricks schema name")

    parser.add_argument(
        "--max-retries", type=int, default=3, help="Maximum number of retries for failed operations"
    )

    parser.add_argument(
        "--retry-delay-seconds", type=int, default=60, help="Delay between retries in seconds"
    )

    # Create subparsers for different modes
    subparsers = parser.add_subparsers(dest="mode", help="Available modes")

    # Autoloader subcommand
    autoloader_parser = subparsers.add_parser("autoloader", help="Run autoloader ingestion")
    autoloader_parser.add_argument(
        "--source-volume-path", required=True, help="Path to source volume containing PDFs"
    )
    autoloader_parser.add_argument(
        "--checkpoint-location", required=True, help="Path for streaming checkpoint"
    )

    autoloader_subparsers = autoloader_parser.add_subparsers(
        dest="autoloader_command", help="Autoloader commands"
    )
    autoloader_subparsers.add_parser("stream", help="Start streaming ingestion")
    autoloader_subparsers.add_parser("rescan", help="One-time rescan of volume")
    autoloader_subparsers.add_parser("stats", help="Get ingestion statistics")

    # OCR subcommand
    ocr_parser = subparsers.add_parser("ocr", help="Run OCR processing")
    ocr_parser.add_argument(
        "--processing-mode",
        choices=["incremental", "reprocess_all", "reprocess_specific"],
        default="incremental",
        help="Processing mode",
    )
    ocr_parser.add_argument(
        "--max-docs-per-run",
        type=int,
        default=100,
        help="Maximum number of documents to process per run",
    )
    ocr_parser.add_argument(
        "--max-pages-per-pdf",
        type=int,
        default=None,
        help="Maximum number of pages to process per PDF",
    )
    ocr_parser.add_argument(
        "--batch-size", type=int, default=10, help="Number of PDFs to process in parallel"
    )
    ocr_parser.add_argument(
        "--specific-file-ids", help="Comma-separated list of file IDs for reprocess_specific mode"
    )
    ocr_parser.add_argument(
        "--model-endpoint-name",
        default="databricks-claude-3-7-sonnet",
        help="Name of the Claude model endpoint",
    )
    ocr_parser.add_argument(
        "--claude-max-tokens", type=int, default=4096, help="Maximum tokens for Claude API"
    )
    ocr_parser.add_argument(
        "--claude-temperature", type=float, default=0.0, help="Temperature for Claude API"
    )
    ocr_parser.add_argument(
        "--image-max-edge-pixels", type=int, default=1568, help="Maximum edge pixels for images"
    )
    ocr_parser.add_argument(
        "--image-dpi", type=int, default=200, help="DPI for PDF to image conversion"
    )

    ocr_subparsers = ocr_parser.add_subparsers(dest="ocr_command", help="OCR commands")
    ocr_subparsers.add_parser("process", help="Process PDFs")
    ocr_subparsers.add_parser("stats", help="Get processing statistics")
    ocr_subparsers.add_parser("failed", help="Show failed files")
    ocr_subparsers.add_parser("reset-failed", help="Reset failed files to pending")
    ocr_subparsers.add_parser("test-claude", help="Test Claude API connection")

    args = parser.parse_args()

    # Set up logging
    logger = setup_logging(args.log_level)

    if not args.mode:
        parser.print_help()
        return 1

    logger.info(f"Starting databricks-pdf-ocr in {args.mode} mode")

    try:
        if args.mode == "autoloader":
            if not args.autoloader_command:
                autoloader_parser.print_help()
                return 1
            return run_autoloader_mode(args, logger)

        elif args.mode == "ocr":
            if not args.ocr_command:
                ocr_parser.print_help()
                return 1
            return run_ocr_mode(args, logger)

        else:
            logger.error(f"Unknown mode: {args.mode}")
            return 1

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        return 0
    except Exception as e:
        logger.error(f"Error running pipeline: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
