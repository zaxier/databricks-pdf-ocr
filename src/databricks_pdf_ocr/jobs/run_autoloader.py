#!/usr/bin/env python3
"""
Databricks job entry point for PDF Autoloader ingestion
"""

import argparse
import sys
from typing import Any

from pyspark.sql import SparkSession

from ..config.settings import AutoloaderConfig
from ..handlers.autoloader import AutoloaderHandler
from ..utils.spark import get_spark_session


def create_autoloader_config(args: argparse.Namespace) -> AutoloaderConfig:
    """Create autoloader configuration from command line arguments"""
    return AutoloaderConfig(
        catalog=args.catalog,
        schema=args.schema,
        source_volume_path=args.source_volume_path,
        checkpoint_location=args.checkpoint_location,
        source_table_path=args.source_table_path,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay_seconds,
        max_file_size_bytes=args.max_file_size_bytes,
        file_format=args.file_format,
    )


def run_autoloader(
    spark: SparkSession, config: AutoloaderConfig, mode: str = "stream"
) -> dict[str, Any]:
    """Run the autoloader ingestion job"""

    print(f"Starting PDF Autoloader in {mode} mode")
    print(f"Source: {config.source_volume_path}")
    print(f"Target: {config.source_table_name}")
    print(f"Checkpoint: {config.checkpoint_path}")

    # Create handler
    handler = AutoloaderHandler(spark, config)

    # Validate volume path
    validation_result = handler.validate_volume_path()
    if not validation_result["valid"]:
        print(f"ERROR: Volume path validation failed: {validation_result['error']}")
        return {"success": False, "error": validation_result["error"]}

    print(f"Volume validation passed: {validation_result['pdf_files']} PDF files found")

    try:
        if mode == "stream":
            # Start streaming job
            result = handler.process()

            # Wait for termination if requested
            if hasattr(handler, "streaming_query") and handler.streaming_query:
                print("Streaming job started. Use Ctrl+C to stop.")
                handler.wait_for_termination()

            return {"success": True, "mode": mode, "result": result.get_summary()}

        elif mode == "rescan":
            # One-time rescan of volume
            result = handler.rescan_volume()
            return {"success": True, "mode": mode, "result": result.get_summary()}

        elif mode == "stats":
            # Get current statistics
            stats = handler.get_ingestion_stats()
            return {"success": True, "mode": mode, "stats": stats}

        else:
            raise ValueError(f"Unknown mode: {mode}")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return {"success": False, "error": str(e)}

    finally:
        # Clean up if needed
        if hasattr(handler, "streaming_query") and handler.streaming_query:
            handler.stop_stream()


def main() -> None:
    """Main entry point for the autoloader job"""
    parser = argparse.ArgumentParser(
        description="Run PDF Autoloader ingestion job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Required arguments
    parser.add_argument("--catalog", required=True, help="Databricks catalog name")

    parser.add_argument("--schema", required=True, help="Databricks schema name")

    parser.add_argument(
        "--source-volume-path", required=True, help="Path to source volume containing PDFs"
    )

    parser.add_argument(
        "--checkpoint-location", required=True, help="Path for streaming checkpoint"
    )

    parser.add_argument("--source-table-path", required=True, help="Path to source Delta table")

    # Optional arguments
    parser.add_argument(
        "--mode", choices=["stream", "rescan", "stats"], default="stream", help="Execution mode"
    )

    parser.add_argument(
        "--max-retries", type=int, default=3, help="Maximum number of retries for failed operations"
    )

    parser.add_argument(
        "--retry-delay-seconds", type=int, default=60, help="Delay between retries in seconds"
    )

    parser.add_argument(
        "--max-file-size-bytes",
        type=int,
        default=None,
        help="Maximum file size to process in bytes",
    )

    parser.add_argument("--file-format", default="binaryFile", help="File format for autoloader")

    parser.add_argument(
        "--cleanup-checkpoint", action="store_true", help="Clean up checkpoint before starting"
    )

    parser.add_argument(
        "--validate-only", action="store_true", help="Only validate configuration, don't run job"
    )

    args = parser.parse_args()

    # Create Spark session
    spark = get_spark_session("PDF_Autoloader")

    try:
        # Create configuration
        config = create_autoloader_config(args)

        # Create handler for validation
        handler = AutoloaderHandler(spark, config)

        # Clean up checkpoint if requested
        if args.cleanup_checkpoint:
            print("Cleaning up checkpoint...")
            handler.cleanup_checkpoint()

        # Validate configuration
        validation_result = handler.validate_volume_path()
        if not validation_result["valid"]:
            print(f"ERROR: Configuration validation failed: {validation_result['error']}")
            sys.exit(1)

        print("Configuration validation passed")

        if args.validate_only:
            print("Validation complete. Exiting.")
            sys.exit(0)

        # Run the job
        result = run_autoloader(spark, config, args.mode)

        if result["success"]:
            print("Job completed successfully")
            if "result" in result:
                print(f"Result: {result['result']}")
            if "stats" in result:
                print(f"Stats: {result['stats']}")
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
