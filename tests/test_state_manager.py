"""Tests for state manager."""

from unittest.mock import Mock, patch

import pytest

from databricks_pdf_ocr.config import OCRProcessingConfig
from databricks_pdf_ocr.managers.state import StateManager


class TestStateManager:
    """Test cases for StateManager."""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session."""
        return Mock()

    @pytest.fixture
    def mock_config(self):
        """Mock OCR processing config."""
        config = Mock(spec=OCRProcessingConfig)
        config.processing_mode = "incremental"
        config.state_table_path = "test.processing_state"
        return config

    @pytest.fixture
    def state_manager(self, mock_spark, mock_config):
        """Create StateManager instance with mocks."""
        return StateManager(mock_spark, mock_config)

    def test_create_run_record(self, state_manager, mock_spark):
        """Test creating a new run record."""
        mock_df = Mock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_write = Mock()
        mock_df.write = mock_write

        run_config = {"mode": "test", "max_docs": 10}

        with patch("uuid.uuid4") as mock_uuid:
            mock_uuid.return_value = "test-run-id"

            run_id = state_manager.create_run_record(run_config)

            assert run_id == "test-run-id"
            mock_spark.createDataFrame.assert_called_once()
            mock_write.mode.assert_called_with("append")

    def test_update_run_record_safe_operations(self, state_manager, mock_spark):
        """Test that update_run_record uses safe DataFrame operations."""
        mock_table_df = Mock()
        mock_spark.table.return_value = mock_table_df

        # Setup chained mock operations
        mock_with_column = Mock()
        mock_table_df.withColumn.return_value = mock_with_column
        mock_with_column.withColumn.return_value = mock_with_column

        mock_drop = Mock()
        mock_with_column.drop.return_value = mock_drop
        mock_write = Mock()
        mock_drop.write = mock_write

        stats = {
            "files_processed": 5,
            "files_succeeded": 4,
            "files_failed": 1,
            "total_pages_processed": 20,
        }

        state_manager.update_run_record("test-run-id", stats, 150.5)

        # Verify safe DataFrame operations were used (no raw SQL)
        mock_spark.table.assert_called_once_with(state_manager.config.state_table_path)
        mock_write.mode.assert_called_with("overwrite")

    def test_get_last_successful_run_exists(self, state_manager, mock_spark):
        """Test getting last successful run when records exist."""
        mock_table_df = Mock()
        mock_spark.table.return_value = mock_table_df

        # Setup chained operations
        mock_filter = Mock()
        mock_order = Mock()
        mock_limit = Mock()

        mock_table_df.filter.return_value = mock_filter
        mock_filter.orderBy.return_value = mock_order
        mock_order.limit.return_value = mock_limit

        # Mock collected row
        mock_row = Mock()
        mock_row.run_id = "last-run-id"
        mock_row.files_processed = 10
        mock_row.configuration = '{"test": "config"}'
        mock_limit.collect.return_value = [mock_row]

        result = state_manager.get_last_successful_run()

        assert result["run_id"] == "last-run-id"
        assert result["files_processed"] == 10
        assert result["configuration"] == {"test": "config"}

    def test_get_last_successful_run_none_exists(self, state_manager, mock_spark):
        """Test getting last successful run when no records exist."""
        mock_table_df = Mock()
        mock_spark.table.return_value = mock_table_df

        # Setup to return empty list
        mock_table_df.filter.return_value.orderBy.return_value.limit.return_value.collect.return_value = []

        result = state_manager.get_last_successful_run()

        assert result == {}

    def test_get_processing_history(self, state_manager, mock_spark):
        """Test getting processing history."""
        mock_table_df = Mock()
        mock_spark.table.return_value = mock_table_df

        # Setup chained operations
        mock_order = Mock()
        mock_limit = Mock()

        mock_table_df.orderBy.return_value = mock_order
        mock_order.limit.return_value = mock_limit

        # Mock collected rows
        mock_rows = [Mock(run_id="run1", files_processed=5), Mock(run_id="run2", files_processed=3)]
        mock_limit.collect.return_value = mock_rows

        result = state_manager.get_processing_history(limit=2)

        assert len(result) == 2
        assert result[0]["run_id"] == "run1"
        assert result[1]["run_id"] == "run2"
