"""Autoloader integration tests - testing real streaming ingestion."""


import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from databricks_pdf_ocr.config import AutoloaderConfig
from databricks_pdf_ocr.handlers.autoloader import AutoloaderHandler
from databricks_pdf_ocr.schemas import create_source_table_sql


class TestAutoloaderIntegration:
    """Integration tests for Autoloader streaming ingestion."""

    @pytest.fixture
    def autoloader_handler(self, spark_session: SparkSession) -> AutoloaderHandler:
        """Create AutoloaderHandler for testing."""
        import uuid
        config = AutoloaderConfig()
        # Use unique checkpoint path for each test to avoid conflicts
        unique_id = str(uuid.uuid4())[:8]
        config.checkpoint_location = f"/Volumes/zaxier_dev/pdf_ocr_test/checkpoints/pdf_ingestion_{unique_id}"
        return AutoloaderHandler(spark_session, config)

    @pytest.mark.integration
    def test_source_table_creation(
        self,
        autoloader_handler: AutoloaderHandler,
        spark_session: SparkSession,
        cleanup_test_tables: None,
        integration_test_marker: bool,
    ) -> None:
        """Test that source table is created correctly."""

        # Create source table
        table_path = autoloader_handler.config.source_table_path
        spark_session.sql(create_source_table_sql(table_path))

        # Verify table exists and has correct schema
        df = spark_session.table(table_path)
        columns = df.columns

        expected_columns = [
            "file_id", "file_name", "file_path", "file_content",
            "modification_time", "ingestion_timestamp", "file_size", "content_hash"
        ]

        for col in expected_columns:
            assert col in columns, f"Column {col} should exist in source table"

        # Verify table is empty initially
        assert df.count() == 0, "Source table should be empty initially"

    @pytest.mark.integration
    def test_checkpoint_volume_creation(
        self,
        autoloader_handler: AutoloaderHandler,
        workspace_client: WorkspaceClient,
        integration_test_marker: bool,
    ) -> None:
        """Test that checkpoint volume is created if it doesn't exist."""

        # Get checkpoint volume info
        checkpoint_info = autoloader_handler.config.checkpoint_volume_info

        # Ensure schema exists (should be created by test fixtures)
        schema_name = f"{checkpoint_info['catalog']}.{checkpoint_info['schema']}"
        schema = workspace_client.schemas.get(schema_name)
        assert schema.name == checkpoint_info['schema']

        # Create checkpoint volume if needed
        autoloader_handler.ensure_checkpoint_volume_exists()

        # Verify volume exists
        volume_name = f"{checkpoint_info['catalog']}.{checkpoint_info['schema']}.{checkpoint_info['volume']}"
        volume = workspace_client.volumes.read(volume_name)
        assert volume.name == checkpoint_info['volume']
