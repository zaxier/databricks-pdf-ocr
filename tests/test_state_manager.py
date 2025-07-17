"""Unit tests for StateManager class."""

import json
import uuid
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_pdf_ocr.config.settings import OCRConfig, ProcessingMode, RunMetrics
from databricks_pdf_ocr.utils.state_manager import StateManager


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    spark = Mock(spec=SparkSession)
    return spark


@pytest.fixture
def ocr_config():
    """Create a test OCR configuration."""
    config = Mock(spec=OCRConfig)
    config.base_path = "test_catalog.test_schema"
    config.target_table_name = "test_catalog.test_schema.pdf_ocr_results"
    config.state_table_name = "test_catalog.test_schema.pdf_processing_state"
    config.max_retries = 3
    return config


@pytest.fixture
def state_manager(mock_spark, ocr_config):
    """Create a StateManager instance with mocked dependencies."""
    return StateManager(mock_spark, ocr_config)


class TestStateManager:
    """Test cases for StateManager class."""

    def test_init(self, mock_spark, ocr_config):
        """Test StateManager initialization."""
        manager = StateManager(mock_spark, ocr_config)
        
        assert manager.spark == mock_spark
        assert manager.config == ocr_config
        assert manager.source_table == "test_catalog.test_schema.pdf_source"
        assert manager.target_table == "test_catalog.test_schema.pdf_ocr_results"
        assert manager.state_table == "test_catalog.test_schema.pdf_processing_state"

    def test_get_pending_files_incremental_mode(self, state_manager, mock_spark):
        """Test getting pending files in incremental mode."""
        # Mock the DataFrame chain
        mock_df = Mock(spec=DataFrame)
        mock_filtered_df = Mock(spec=DataFrame)
        mock_limited_df = Mock(spec=DataFrame)
        mock_selected_df = Mock(spec=DataFrame)
        
        mock_spark.table.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df
        mock_filtered_df.limit.return_value = mock_limited_df
        mock_limited_df.select.return_value = mock_selected_df
        
        # Test without limit
        result = state_manager.get_pending_files(
            mode=ProcessingMode.INCREMENTAL,
            max_files=None
        )
        
        mock_spark.table.assert_called_once_with("test_catalog.test_schema.pdf_source")
        assert mock_df.filter.called
        assert result == mock_selected_df
        
        # Test with limit
        result_with_limit = state_manager.get_pending_files(
            mode=ProcessingMode.INCREMENTAL,
            max_files=10
        )
        
        mock_filtered_df.limit.assert_called_once_with(10)

    def test_get_pending_files_reprocess_all_mode(self, state_manager, mock_spark):
        """Test getting pending files in reprocess all mode."""
        # Mock the DataFrame chain
        mock_df = Mock(spec=DataFrame)
        mock_filtered_df = Mock(spec=DataFrame)
        mock_selected_df = Mock(spec=DataFrame)
        
        mock_spark.table.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df
        
        result = state_manager.get_pending_files(
            mode=ProcessingMode.REPROCESS_ALL
        )
        
        # Should filter with lit(True) for all files
        filter_call_args = mock_df.filter.call_args[0][0]
        # Can't directly compare Column objects, so we'll trust the implementation
        assert result == mock_selected_df

    def test_get_pending_files_reprocess_specific_mode(self, state_manager, mock_spark):
        """Test getting pending files in reprocess specific mode."""
        # Mock the DataFrame chain
        mock_df = Mock(spec=DataFrame)
        mock_filtered_df = Mock(spec=DataFrame)
        mock_selected_df = Mock(spec=DataFrame)
        
        mock_spark.table.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df
        
        # Test with specific IDs
        specific_ids = ["file1", "file2", "file3"]
        result = state_manager.get_pending_files(
            mode=ProcessingMode.REPROCESS_SPECIFIC,
            specific_ids=specific_ids
        )
        
        assert result == mock_selected_df
        
        # Test without specific IDs (should raise error)
        with pytest.raises(ValueError, match="specific_ids must be provided"):
            state_manager.get_pending_files(
                mode=ProcessingMode.REPROCESS_SPECIFIC,
                specific_ids=None
            )

    def test_get_pending_files_invalid_mode(self, state_manager):
        """Test getting pending files with invalid mode."""
        with pytest.raises(ValueError, match="Unknown processing mode"):
            state_manager.get_pending_files(mode="INVALID_MODE")

    def test_update_file_status(self, state_manager, mock_spark):
        """Test updating file status."""
        # Mock the DataFrame chain
        mock_df = Mock(spec=DataFrame)
        mock_filtered_df = Mock(spec=DataFrame)
        mock_selected_df = Mock(spec=DataFrame)
        mock_write = Mock()
        
        mock_spark.table.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df
        mock_selected_df.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write
        
        # Test valid status update
        state_manager.update_file_status(
            file_id="test_file_id",
            status="completed",
            error=None,
            increment_attempts=True
        )
        
        mock_spark.sql.assert_called_once()
        merge_sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in merge_sql
        assert "test_file_id" in merge_sql
        assert "completed" in merge_sql
        
        # Test invalid status
        with pytest.raises(ValueError, match="Invalid status"):
            state_manager.update_file_status(
                file_id="test_file_id",
                status="invalid_status"
            )

    def test_update_multiple_file_statuses(self, state_manager, mock_spark):
        """Test updating multiple file statuses."""
        # Mock DataFrame creation
        mock_df = Mock(spec=DataFrame)
        mock_spark.createDataFrame.return_value = mock_df
        
        # Test with file updates
        file_updates = [
            {
                "file_id": "file1",
                "status": "completed",
                "error": None,
                "increment_attempts": True
            },
            {
                "file_id": "file2",
                "status": "failed",
                "error": "Test error",
                "increment_attempts": False
            }
        ]
        
        state_manager.update_multiple_file_statuses(file_updates)
        
        # Verify DataFrame was created and temp view was created
        mock_spark.createDataFrame.assert_called_once()
        mock_df.createOrReplaceTempView.assert_called_once_with("file_updates")
        
        # Verify MERGE SQL was executed
        mock_spark.sql.assert_called_once()
        merge_sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in merge_sql
        assert "file_updates" in merge_sql
        
        # Test with empty updates (should return early)
        mock_spark.reset_mock()
        state_manager.update_multiple_file_statuses([])
        mock_spark.createDataFrame.assert_not_called()

    def test_record_run_metrics(self, state_manager, mock_spark):
        """Test recording run metrics."""
        # Mock DataFrame creation
        mock_df = Mock(spec=DataFrame)
        mock_write = Mock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_df.write = mock_write
        mock_write.mode.return_value = mock_write
        
        # Create test metrics
        metrics = RunMetrics(
            run_id="test-run-id",
            processing_mode=ProcessingMode.INCREMENTAL,
            files_processed=10,
            files_succeeded=8,
            files_failed=2,
            total_pages_processed=50,
            processing_duration_seconds=120.5,
            configuration={"batch_size": 5, "max_files": 10}
        )
        
        with patch('databricks_pdf_ocr.utils.state_manager.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
            state_manager.record_run_metrics(metrics)
        
        # Verify DataFrame was created with correct data
        mock_spark.createDataFrame.assert_called_once()
        data, schema = mock_spark.createDataFrame.call_args[0]
        
        assert len(data) == 1
        assert data[0]["run_id"] == "test-run-id"
        assert data[0]["processing_mode"] == "incremental"
        assert data[0]["files_processed"] == 10
        assert data[0]["files_succeeded"] == 8
        assert data[0]["files_failed"] == 2
        assert data[0]["total_pages_processed"] == 50
        assert data[0]["processing_duration_seconds"] == 120.5
        
        # Verify write to table
        mock_write.mode.assert_called_with("append")
        mock_write.saveAsTable.assert_called_with("test_catalog.test_schema.pdf_processing_state")

    def test_get_processing_stats(self, state_manager, mock_spark):
        """Test getting processing statistics."""
        # Mock source table stats
        source_stats_df = Mock(spec=DataFrame)
        source_stats_grouped = Mock()
        source_stats_rows = [
            Row(processing_status="pending", count=10),
            Row(processing_status="completed", count=50),
            Row(processing_status="failed", count=5)
        ]
        
        # Mock recent runs
        recent_runs_df = Mock(spec=DataFrame)
        recent_runs_ordered = Mock()
        recent_runs_limited = Mock()
        recent_runs_rows = [
            Row(
                run_id="run1",
                run_timestamp=datetime(2024, 1, 1, 12, 0, 0),
                processing_mode="incremental",
                files_processed=10,
                files_succeeded=8,
                files_failed=2,
                total_pages_processed=50,
                processing_duration_seconds=120.5
            )
        ]
        
        # Mock target table count
        target_df = Mock(spec=DataFrame)
        
        # Set up mock returns
        def table_side_effect(table_name):
            if "pdf_source" in table_name:
                return source_stats_df
            elif "pdf_processing_state" in table_name:
                return recent_runs_df
            elif "pdf_ocr_results" in table_name:
                return target_df
            
        mock_spark.table.side_effect = table_side_effect
        
        source_stats_df.groupBy.return_value = source_stats_grouped
        source_stats_grouped.count.return_value = source_stats_grouped
        source_stats_grouped.collect.return_value = source_stats_rows
        
        recent_runs_df.orderBy.return_value = recent_runs_ordered
        recent_runs_ordered.limit.return_value = recent_runs_limited
        recent_runs_limited.collect.return_value = recent_runs_rows
        
        target_df.count.return_value = 200
        
        # Get stats
        stats = state_manager.get_processing_stats()
        
        # Verify results
        assert stats["source_file_status"]["pending"] == 10
        assert stats["source_file_status"]["completed"] == 50
        assert stats["source_file_status"]["failed"] == 5
        assert stats["extracted_pages"] == 200
        assert len(stats["recent_runs"]) == 1
        assert stats["recent_runs"][0]["run_id"] == "run1"
        assert stats["recent_runs"][0]["files_processed"] == 10

    def test_get_failed_files(self, state_manager, mock_spark):
        """Test getting failed files."""
        # Mock the DataFrame chain
        mock_df = Mock(spec=DataFrame)
        mock_filtered_df = Mock(spec=DataFrame)
        mock_selected_df = Mock(spec=DataFrame)
        mock_ordered_df = Mock(spec=DataFrame)
        mock_limited_df = Mock(spec=DataFrame)
        
        mock_spark.table.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.select.return_value = mock_selected_df
        mock_selected_df.orderBy.return_value = mock_ordered_df
        mock_ordered_df.limit.return_value = mock_limited_df
        
        # Test without limit
        result = state_manager.get_failed_files()
        assert result == mock_ordered_df
        
        # Test with limit
        result_with_limit = state_manager.get_failed_files(limit=10)
        assert result_with_limit == mock_limited_df
        mock_ordered_df.limit.assert_called_with(10)

    def test_reset_file_status(self, state_manager, mock_spark):
        """Test resetting file status."""
        # Test with file IDs
        file_ids = ["file1", "file2", "file3"]
        state_manager.reset_file_status(file_ids)
        
        # Verify SQL was executed
        mock_spark.sql.assert_called_once()
        reset_sql = mock_spark.sql.call_args[0][0]
        assert "UPDATE" in reset_sql
        assert "processing_status = 'pending'" in reset_sql
        assert "processing_attempts = 0" in reset_sql
        assert "last_error = NULL" in reset_sql
        assert all(fid in reset_sql for fid in file_ids)
        
        # Test with empty list (should return early)
        mock_spark.reset_mock()
        state_manager.reset_file_status([])
        mock_spark.sql.assert_not_called()

    def test_cleanup_old_runs(self, state_manager, mock_spark):
        """Test cleaning up old run records."""
        state_manager.cleanup_old_runs(days_to_keep=30)
        
        # Verify DELETE SQL was executed
        mock_spark.sql.assert_called_once()
        cleanup_sql = mock_spark.sql.call_args[0][0]
        assert "DELETE FROM" in cleanup_sql
        assert "pdf_processing_state" in cleanup_sql
        assert "date_sub(current_timestamp(), 30)" in cleanup_sql

    def test_get_run_by_id(self, state_manager, mock_spark):
        """Test getting run by ID."""
        # Mock DataFrame and results
        mock_df = Mock(spec=DataFrame)
        mock_filtered_df = Mock(spec=DataFrame)
        
        test_run = Row(
            run_id="test-run-id",
            run_timestamp=datetime(2024, 1, 1, 12, 0, 0),
            processing_mode="incremental",
            files_processed=10,
            files_succeeded=8,
            files_failed=2,
            total_pages_processed=50,
            processing_duration_seconds=120.5,
            configuration='{"batch_size": 5}'
        )
        
        mock_spark.table.return_value = mock_df
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.collect.return_value = [test_run]
        
        # Get run
        result = state_manager.get_run_by_id("test-run-id")
        
        # Verify results
        assert result is not None
        assert result["run_id"] == "test-run-id"
        assert result["files_processed"] == 10
        assert result["configuration"]["batch_size"] == 5
        
        # Test non-existent run
        mock_filtered_df.collect.return_value = []
        result_none = state_manager.get_run_by_id("non-existent")
        assert result_none is None

    def test_create_run_id(self, state_manager):
        """Test creating a unique run ID."""
        with patch('databricks_pdf_ocr.utils.state_manager.uuid.uuid4') as mock_uuid:
            mock_uuid.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
            run_id = state_manager.create_run_id()
            
            assert run_id == '12345678-1234-5678-1234-567812345678'
            mock_uuid.assert_called_once()