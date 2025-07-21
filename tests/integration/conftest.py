"""Integration test configuration and fixtures."""

import json
import os
import signal
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from pyspark.sql import SparkSession

from databricks_pdf_ocr.config import (
    AutoloaderConfig,
    OCRProcessingConfig,
    SyncConfig,
    create_spark_session,
)
from databricks_pdf_ocr.main import PDFOCRPipeline


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment() -> Generator[None, None, None]:
    """Set up test environment and clean up after all tests."""
    # Set environment to testing
    original_env = os.environ.get("PDF_OCR_ENV")
    os.environ["PDF_OCR_ENV"] = "testing"

    yield

    # Restore original environment
    if original_env is not None:
        os.environ["PDF_OCR_ENV"] = original_env
    else:
        os.environ.pop("PDF_OCR_ENV", None)


@pytest.fixture(scope="function")
def spark_session() -> Generator[SparkSession, None, None]:
    """Create a Databricks Spark session for integration tests."""
    spark = create_spark_session()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    """Create a Databricks workspace client."""
    return WorkspaceClient(profile="DEFAULT")


@pytest.fixture(scope="session")
def test_volume_setup(workspace_client: WorkspaceClient) -> Generator[dict[str, str], None, None]:
    """Set up test volume in Databricks and clean up after tests."""
    # Get volume info from actual configuration instead of hardcoding
    sync_config = SyncConfig()
    volume_info = {
        "catalog": sync_config.catalog,
        "schema": sync_config.schema,
        "volume": sync_config.volume,
    }

    # Ensure schema exists
    try:
        workspace_client.schemas.get(f"{volume_info['catalog']}.{volume_info['schema']}")
    except Exception:
        workspace_client.schemas.create(
            name=volume_info["schema"], catalog_name=volume_info["catalog"]
        )

    # Ensure volume exists
    try:
        workspace_client.volumes.read(
            f"{volume_info['catalog']}.{volume_info['schema']}.{volume_info['volume']}"
        )
    except Exception:
        workspace_client.volumes.create(
            catalog_name=volume_info["catalog"],
            schema_name=volume_info["schema"],
            name=volume_info["volume"],
            volume_type=VolumeType.MANAGED,
        )

    # Clean up before tests start
    _cleanup_test_files(workspace_client, volume_info)

    yield volume_info

    # Cleanup: Remove test files from volume after tests
    _cleanup_test_files(workspace_client, volume_info)


def _cleanup_test_files(workspace_client: WorkspaceClient, volume_info: dict[str, str]) -> None:
    """Clean up test files from volume with timeout and better error handling."""

    def timeout_handler(signum, frame):
        raise TimeoutError("Cleanup operation timed out")

    volume_path = (
        f"/Volumes/{volume_info['catalog']}/{volume_info['schema']}/{volume_info['volume']}"
    )

    try:
        # Set a 30-second timeout for cleanup operations
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(30)

        try:
            files = workspace_client.files.list_directory_contents(volume_path)
            pdf_files = [
                file
                for file in files
                if file.name
                and file.name.endswith(".pdf")
                and file.path
                and (
                    "test_" in file.name
                    or "multi_ingestion_" in file.name
                    or "single_ingestion_" in file.name
                )
            ]

            # Delete PDF files one by one with individual error handling
            for file in pdf_files:
                if file.path:
                    try:
                        workspace_client.files.delete(file.path)
                    except Exception as e:
                        print(f"Warning: Failed to delete {file.path}: {e}")

        except TimeoutError:
            print("Warning: Cleanup timed out - some files may remain")
        except Exception as e:
            print(f"Warning: Cleanup failed with error: {e}")
        finally:
            signal.alarm(0)  # Cancel the alarm

    except Exception as e:
        print(f"Warning: Could not set up cleanup timeout: {e}")
        # Fallback: try simple cleanup without timeout
        try:
            files = workspace_client.files.list_directory_contents(volume_path)
            for file in files:
                if (
                    file.name
                    and file.name.endswith(".pdf")
                    and file.path
                    and ("test_" in file.name or "multi_ingestion_" in file.name)
                ):
                    try:
                        workspace_client.files.delete(file.path)
                    except Exception:
                        pass
        except Exception:
            pass  # Ignore all cleanup errors in fallback


@pytest.fixture
def cleanup_test_tables(spark_session: SparkSession) -> Generator[None, None, None]:
    """Clean up test tables before and after each test."""
    # Get table names from actual configuration instead of hardcoding
    ocr_config = OCRProcessingConfig()
    autoloader_config = AutoloaderConfig()

    test_tables = [
        autoloader_config.source_table_path,
        ocr_config.target_table_path,
        ocr_config.state_table_path,
    ]

    # Clean up before test
    for table in test_tables:
        try:
            spark_session.sql(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass

    yield

    # Clean up after test
    for table in test_tables:
        try:
            spark_session.sql(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass


@pytest.fixture
def test_pipeline(spark_session: SparkSession, cleanup_test_tables: None) -> PDFOCRPipeline:
    """Create a PDFOCRPipeline instance configured for testing."""
    return PDFOCRPipeline(spark_session)


@pytest.fixture
def mock_claude_responses() -> dict[str, Any]:
    """Load mock Claude API responses from fixtures."""
    fixtures_path = Path(__file__).parent / "fixtures" / "mock_responses.json"
    with open(fixtures_path) as f:
        return json.load(f)


@pytest.fixture
def test_pdf_files() -> dict[str, Path]:
    """Get paths to test PDF files."""
    fixtures_dir = Path(__file__).parent.parent.parent / "fixtures"
    return {
        "dictionary": fixtures_dir / "dictionary.pdf",
        "invoice": fixtures_dir / "invoice.pdf",
        "textbook": fixtures_dir / "textbook.pdf",
    }


@pytest.fixture
def temp_local_dir() -> Generator[Path, None, None]:
    """Create temporary directory for local file operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def checkpoint_cleanup(workspace_client: WorkspaceClient) -> Generator[None, None, None]:
    """Clean up autoloader checkpoints before and after tests."""
    # Get checkpoint path from actual configuration instead of hardcoding
    autoloader_config = AutoloaderConfig()
    checkpoint_volume_info = autoloader_config.checkpoint_volume_info
    checkpoint_path = f"/Volumes/{checkpoint_volume_info['catalog']}/{checkpoint_volume_info['schema']}/{checkpoint_volume_info['volume']}"

    def cleanup_recursive(path: str) -> None:
        """Recursively clean up checkpoint files and directories."""
        try:
            files = workspace_client.files.list_directory_contents(path)
            for file in files:
                if file.path:
                    if file.is_directory:
                        # Recursively clean subdirectories
                        cleanup_recursive(file.path)
                    else:
                        # Delete files
                        try:
                            workspace_client.files.delete(file.path)
                        except Exception:
                            pass
        except Exception:
            pass

    # Clean up before test
    cleanup_recursive(checkpoint_path)

    yield

    # Clean up after test
    cleanup_recursive(checkpoint_path)


@pytest.fixture
def integration_test_marker():
    """Marker for integration tests that require real Databricks environment."""
    pytest.importorskip("databricks.connect")
    return True


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests requiring Databricks environment"
    )


def pytest_collection_modifyitems(config, items):
    """Add integration marker to all tests in integration directory."""
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
