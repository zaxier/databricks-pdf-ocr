import argparse
import logging


def main() -> int:
    """Main entry point for the databricks-pdf-ocr application."""
    parser = argparse.ArgumentParser(description="Databricks PDF OCR Pipeline")
    parser.add_argument(
        "--mode",
        choices=["autoloader", "ocr", "both"],
        default="ocr",
        help="Processing mode to run",
    )
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Starting databricks-pdf-ocr in {args.mode} mode")

    try:
        if args.mode == "autoloader":
            logger.info("Running autoloader ingestion")
            # TODO: Implement autoloader logic
        elif args.mode == "ocr":
            logger.info("Running OCR processing")
            # TODO: Implement OCR processing logic
        else:
            logger.info("Running both autoloader and OCR processing")
            # TODO: Implement combined logic

        return 0
    except Exception as e:
        logger.error(f"Error running pipeline: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
