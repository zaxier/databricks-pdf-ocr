"""Command-line interface for pdf-parse-claude."""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from .databricks_claude_ocr import DatabricksClaudeOCR


def main() -> int:
    """Main CLI entry point."""
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Extract text from PDFs using Databricks Claude endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "pdf_path",
        type=str,
        help="Path to the PDF file to process",
    )

    parser.add_argument(
        "-o", "--output",
        type=str,
        help="Output file path (default: stdout)",
    )

    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv('DATABRICKS_HOST'),
        help="Databricks host URL (defaults to DATABRICKS_HOST env var)",
    )

    parser.add_argument(
        "--token",
        type=str,
        default=os.getenv('DATABRICKS_ACCESS_TOKEN'),
        help="Databricks access token (defaults to DATABRICKS_ACCESS_TOKEN env var)",
    )

    parser.add_argument(
        "--endpoint-name",
        type=str,
        default=os.getenv('DATABRICKS_ENDPOINT_NAME', 'databricks-claude-3-7-sonnet'),
        help="Model serving endpoint name (defaults to DATABRICKS_ENDPOINT_NAME)",
    )


    parser.add_argument(
        "--page-limit",
        type=int,
        help="Maximum number of pages to process",
    )


    args = parser.parse_args()

    # Validate PDF path
    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        print(f"Error: PDF file not found: {pdf_path}", file=sys.stderr)
        return 1

    # Validate host and token
    if not args.host:
        print(
            "Error: Databricks host not provided. "
            "Use --host or set DATABRICKS_HOST in .env",
            file=sys.stderr
        )
        return 1

    if not args.token:
        print(
            "Error: Databricks token not provided. "
            "Use --token or set DATABRICKS_ACCESS_TOKEN in .env",
            file=sys.stderr
        )
        return 1

    try:
        # Initialize OCR instance
        ocr = DatabricksClaudeOCR(
            workspace_url=args.host,
            token=args.token,
            endpoint_name=args.endpoint_name,
        )

        # Extract text
        print("Extracting text from PDF...", file=sys.stderr)
        extracted_text = ocr.extract_text_from_pdf(
            pdf_path=str(pdf_path),
            max_pages=args.page_limit,
        )

        # Output results
        if args.output:
            output_path = Path(args.output)
            output_path.write_text(extracted_text, encoding="utf-8")
            print(f"Text extracted successfully to: {output_path}", file=sys.stderr)
        else:
            print(extracted_text)

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
