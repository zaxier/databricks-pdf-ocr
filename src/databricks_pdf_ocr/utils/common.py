"""
Common utilities to reduce code duplication across the PDF OCR pipeline
"""

import argparse
import logging
import sys
from typing import Any, Dict, List

from ..handlers.base import ProcessingResult


def setup_logging(log_level: str, app_name: str = "DatabricksPDFOCR") -> logging.Logger:
    """
    Set up logging configuration with standardized format
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        app_name: Application name for the logger
        
    Returns:
        Configured logger instance
    """
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=f"%(asctime)s - {app_name} - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    return logging.getLogger(app_name)


def validate_required_args(args: argparse.Namespace, required_fields: List[str]) -> List[str]:
    """
    Validate that required arguments are present
    
    Args:
        args: Parsed command line arguments
        required_fields: List of required field names
        
    Returns:
        List of missing required arguments
    """
    return [field for field in required_fields if not getattr(args, field, None)]


def display_processing_result(result: ProcessingResult, logger: logging.Logger) -> None:
    """
    Display processing results in a standardized format
    
    Args:
        result: Processing result to display
        logger: Logger instance for output
    """
    summary = result.get_summary()
    
    logger.info(f"Processing completed: {result.run_id}")
    print(f"Files processed: {summary['files_processed']}")
    print(f"Files succeeded: {summary['files_succeeded']}")
    print(f"Files failed: {summary['files_failed']}")
    print(f"Pages processed: {summary['pages_processed']}")
    print(f"Success rate: {summary['success_rate_percent']:.1f}%")
    print(f"Duration: {summary['duration_seconds']:.2f}s")


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add common command line arguments used across multiple entry points
    
    Args:
        parser: ArgumentParser to add arguments to
    """
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
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level"
    )


def add_autoloader_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add autoloader-specific command line arguments
    
    Args:
        parser: ArgumentParser to add arguments to
    """
    parser.add_argument(
        "--source-volume-path",
        required=True,
        help="Path to source volume containing PDFs"
    )
    
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Path for streaming checkpoint"
    )
    
    parser.add_argument(
        "--max-files-per-trigger",
        type=int,
        default=1000,
        help="Maximum files to process per trigger"
    )
    
    parser.add_argument(
        "--use-notifications",
        action="store_true",
        help="Use file notifications instead of directory listing"
    )
    
    parser.add_argument(
        "--backfill-interval",
        default="1 day",
        help="Interval for backfill operations"
    )
    
    parser.add_argument(
        "--schema-location",
        help="Custom schema location for autoloader"
    )


def add_ocr_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add OCR-specific command line arguments
    
    Args:
        parser: ArgumentParser to add arguments to
    """
    parser.add_argument(
        "--processing-mode",
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


def handle_job_error(operation_name: str, error: Exception, logger: logging.Logger) -> int:
    """
    Standardized error handling for job operations
    
    Args:
        operation_name: Name of the operation that failed
        error: Exception that occurred
        logger: Logger instance
        
    Returns:
        Exit code (1 for error)
    """
    logger.error(f"Error in {operation_name}: {str(error)}", exc_info=True)
    return 1


def create_configuration_dict(config_obj: Any) -> Dict[str, Any]:
    """
    Convert configuration object to dictionary for logging/tracking
    
    Args:
        config_obj: Configuration object with attributes
        
    Returns:
        Dictionary representation of configuration
    """
    if hasattr(config_obj, '__dataclass_fields__'):
        # Handle dataclass objects
        from dataclasses import asdict
        return asdict(config_obj)
    else:
        # Handle regular objects by extracting non-private attributes
        return {
            key: value for key, value in vars(config_obj).items()
            if not key.startswith('_')
        }


def format_file_stats(stats: Dict[str, Any]) -> str:
    """
    Format file statistics for display
    
    Args:
        stats: Statistics dictionary
        
    Returns:
        Formatted string representation
    """
    lines = []
    
    if "source_file_status" in stats:
        lines.append("File Status Counts:")
        for status, count in stats["source_file_status"].items():
            lines.append(f"  {status}: {count}")
    
    if "extracted_pages" in stats:
        lines.append(f"Total extracted pages: {stats['extracted_pages']}")
    
    if "recent_runs" in stats:
        lines.append(f"Recent runs: {len(stats['recent_runs'])}")
        for run in stats["recent_runs"][:3]:  # Show last 3 runs
            lines.append(f"  {run['timestamp']}: {run['files_processed']} files processed")
    
    return "\n".join(lines)


def ensure_spark_session_stopped(spark_session):
    """
    Safely stop Spark session if it exists
    
    Args:
        spark_session: Spark session instance
    """
    try:
        if spark_session:
            spark_session.stop()
    except Exception:
        # Ignore errors during cleanup
        pass