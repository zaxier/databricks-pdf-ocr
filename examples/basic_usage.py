"""Basic usage example for pdf-parse-claude."""

from dotenv import load_dotenv, dotenv_values

from pdf_parse_claude import DatabricksClaudeOCR


def main():
    """Demonstrate basic usage of DatabricksClaudeOCR."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Force use of .env file values to override system environment variables
    config = dotenv_values(".env")
    databricks_host = config.get("DATABRICKS_HOST", "https://your-workspace.databricks.com")
    databricks_token = config.get("DATABRICKS_ACCESS_TOKEN", "your-token-here")
    
    print(f"Using credentials from .env file:")
    print(f"  Host: {databricks_host}")
    print(f"  Token: {databricks_token[:15]}..." if databricks_token else "  Token: Not set")

    # Initialize the OCR client
    ocr = DatabricksClaudeOCR(
        workspace_url=databricks_host,
        token=databricks_token
    )

    # Extract text from a PDF
    pdf_path = "./data/example.pdf"

    try:
        # Extract text with default settings
        extracted_text = ocr.extract_text_from_pdf(pdf_path)
        print("Extracted text:")
        print(extracted_text)

        # Save to file
        output_path = pdf_path.replace(".pdf", "_extracted.txt")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(extracted_text)
        print(f"\nText saved to: {output_path}")

    except FileNotFoundError:
        print(f"Error: PDF file not found: {pdf_path}")
    except Exception as e:
        print(f"Error during extraction: {e}")


def extract_with_options():
    """Demonstrate extraction with custom options."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Force use of .env file values to override system environment variables
    config = dotenv_values("../.env")
    
    # Initialize OCR client (configuration assumed to be in environment)
    ocr = DatabricksClaudeOCR(
        workspace_url=config.get("DATABRICKS_HOST"),
        token=config.get("DATABRICKS_ACCESS_TOKEN")
    )

    # Extract with page limit
    extracted_text = ocr.extract_text_from_pdf(
        pdf_path="../data/example.pdf",
        max_pages=5  # Only process first 5 pages
    )

    return extracted_text


if __name__ == "__main__":
    main()
