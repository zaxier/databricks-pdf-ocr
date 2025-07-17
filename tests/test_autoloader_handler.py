"""Unit tests for autoloader handler."""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch, call
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from databricks_pdf_ocr.handlers.autoloader import AutoloaderHandler
from databricks_pdf_ocr.config.settings import AutoloaderConfig


class TestAutoloaderHandler:
    """Test cases for AutoloaderHandler class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = Mock(spec=SparkSession)
        spark.version = "3.5.0"
        
        # Mock table method
        mock_table = Mock()
        spark.table = Mock(return_value=mock_table)
        
        # Mock readStream
        spark.readStream = Mock()
        
        return spark

    @pytest.fixture
    def mock_config(self):
        """Create a mock AutoloaderConfig."""
        config = Mock(spec=AutoloaderConfig)
        config.__class__.__name__ = "AutoloaderConfig"
        config.source_table_name = "test_catalog.test_schema.pdf_source"
        config.source_volume_path = "/Volumes/test/test/pdf_documents"
        config.checkpoint_path = "/Volumes/test/test/checkpoints/autoloader"
        config.schema_location_path = "/Volumes/test/test/schema/autoloader"
        config.file_format = "binaryFile"
        config.include_existing_files = True
        config.max_files_per_trigger = 100
        config.use_notifications = False
        config.backfill_interval = "1 day"
        config.max_file_size_bytes = None
        config.to_dict = Mock(return_value={"test": "config"})
        
        return config

    @pytest.fixture
    def mock_workspace_client(self):
        """Create a mock WorkspaceClient."""
        client = Mock()
        client.dbutils = Mock()
        client.dbutils.fs = Mock()
        return client

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_handler_initialization(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test handler initialization."""
        mock_ws_client.return_value = mock_workspace_client
        
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        assert handler.spark == mock_spark
        assert handler.config == mock_config
        assert handler.streaming_query is None
        assert handler.db_client is not None
        
        # Verify source table creation was called
        mock_create_table.assert_called_once_with(
            mock_spark,
            mock_config.source_table_name,
            location=None
        )

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_process_success(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test successful processing start."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock streaming query
        mock_query = Mock(spec=StreamingQuery)
        handler.start_stream = Mock(return_value=mock_query)
        
        result = handler.process(run_id="test_run_123")
        
        assert result.run_id == "test_run_123"
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.configuration == {"test": "config"}
        
        # Verify stream was started
        handler.start_stream.assert_called_once()

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_process_error_handling(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test error handling during processing."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Make start_stream raise an exception
        handler.start_stream = Mock(side_effect=Exception("Stream start failed"))
        
        with pytest.raises(Exception, match="Stream start failed"):
            handler.process()

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_start_stream(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test starting the autoloader stream."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock the streaming chain
        mock_format = Mock()
        mock_options = Mock()
        mock_load = Mock()
        mock_filter = Mock()
        mock_writeStream = Mock()
        mock_format_delta = Mock()
        mock_outputMode = Mock()
        mock_option = Mock()
        mock_trigger = Mock()
        mock_start = Mock()
        
        # Set up the chain
        mock_spark.readStream.format.return_value = mock_format
        mock_format.options.return_value = mock_options
        mock_options.load.return_value = mock_load
        mock_load.filter.return_value = mock_filter
        
        # Mock transform
        mock_transformed = Mock()
        handler._transform_autoloader_data = Mock(return_value=mock_transformed)
        
        # Set up write chain
        mock_transformed.writeStream = mock_writeStream
        mock_writeStream.format.return_value = mock_format_delta
        mock_format_delta.outputMode.return_value = mock_outputMode
        mock_outputMode.option.return_value = mock_option
        mock_option.option.return_value = mock_trigger
        mock_trigger.trigger.return_value = mock_start
        
        # Mock the final streaming query
        mock_query = Mock(spec=StreamingQuery)
        mock_start.start.return_value = mock_query
        
        result = handler.start_stream()
        
        assert result == mock_query
        
        # Verify autoloader options
        expected_options = {
            "cloudFiles.format": "binaryFile",
            "cloudFiles.includeExistingFiles": "true",
            "cloudFiles.maxFilesPerTrigger": "100",
            "cloudFiles.useNotifications": "false",
            "cloudFiles.schemaLocation": mock_config.schema_location_path,
            "cloudFiles.backfillInterval": "1 day",
            "cloudFiles.validateOptions": "false",
            "cloudFiles.schemaHints": "path STRING, modificationTime TIMESTAMP, length LONG, content BINARY"
        }
        mock_format.options.assert_called_once_with(**expected_options)
        
        # Verify load was called with correct path
        mock_options.load.assert_called_once_with(mock_config.source_volume_path)

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_transform_autoloader_data(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test transforming autoloader data."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock DataFrame
        mock_df = Mock(spec=DataFrame)
        mock_select = Mock()
        mock_df.select = Mock(return_value=mock_select)
        
        result = handler._transform_autoloader_data(mock_df)
        
        assert result == mock_select
        # Verify select was called with proper transformations
        mock_df.select.assert_called_once()

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_stop_stream(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client, capsys):
        """Test stopping the streaming query."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock streaming query
        mock_query = Mock(spec=StreamingQuery)
        handler.streaming_query = mock_query
        
        handler.stop_stream()
        
        mock_query.stop.assert_called_once()
        assert handler.streaming_query is None
        
        captured = capsys.readouterr()
        assert "Stopping autoloader stream..." in captured.out
        assert "Autoloader stream stopped" in captured.out

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_get_stream_status_stopped(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test getting stream status when stopped."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        status = handler.get_stream_status()
        
        assert status["status"] == "stopped"
        assert status["isActive"] is False

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_get_stream_status_running(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test getting stream status when running."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock streaming query
        mock_query = Mock(spec=StreamingQuery)
        mock_query.isActive = True
        mock_query.id = "query_123"
        mock_query.name = "test_query"
        mock_query.lastProgress = {"test": "progress"}
        mock_query.status = {"message": "Running"}
        
        handler.streaming_query = mock_query
        
        status = handler.get_stream_status()
        
        assert status["status"] == "running"
        assert status["isActive"] is True
        assert status["id"] == "query_123"
        assert status["name"] == "test_query"
        assert status["lastProgress"] == {"test": "progress"}
        assert status["status_details"] == {"message": "Running"}

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_wait_for_termination(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test waiting for stream termination."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock streaming query
        mock_query = Mock(spec=StreamingQuery)
        handler.streaming_query = mock_query
        
        handler.wait_for_termination(timeout=60)
        
        mock_query.awaitTermination.assert_called_once_with(60)

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_get_ingestion_stats(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test getting ingestion statistics."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock table and query results
        mock_table = Mock()
        mock_spark.table.return_value = mock_table
        
        # Mock status counts
        mock_status_counts = [
            Mock(processing_status="pending", count=50),
            Mock(processing_status="completed", count=30),
            Mock(processing_status="failed", count=5),
        ]
        mock_groupBy = Mock()
        mock_count = Mock()
        mock_table.groupBy.return_value = mock_groupBy
        mock_groupBy.count.return_value = mock_count
        mock_count.collect.return_value = mock_status_counts
        
        # Mock total stats
        mock_total_stats = Mock()
        mock_total_stats.__getitem__ = lambda self, key: {
            "count(file_id)": 85,
            "sum(file_size)": 1024000
        }[key]
        mock_agg = Mock()
        mock_table.agg.return_value = mock_agg
        mock_agg.collect.return_value = [mock_total_stats]
        
        # Mock recent files
        mock_recent_files = [
            Mock(
                file_name="test1.pdf",
                file_size=1024,
                ingestion_timestamp=datetime.now(),
                processing_status="pending"
            )
        ]
        mock_orderBy = Mock()
        mock_limit = Mock()
        mock_select = Mock()
        mock_table.orderBy.return_value = mock_orderBy
        mock_orderBy.limit.return_value = mock_limit
        mock_limit.select.return_value = mock_select
        mock_select.collect.return_value = mock_recent_files
        
        stats = handler.get_ingestion_stats()
        
        assert stats["total_files"] == 85
        assert stats["total_size_bytes"] == 1024000
        assert stats["status_counts"]["pending"] == 50
        assert stats["status_counts"]["completed"] == 30
        assert stats["status_counts"]["failed"] == 5
        assert len(stats["recent_files"]) == 1

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_rescan_volume(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test rescanning volume for new files."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock read operations
        mock_read = Mock()
        mock_format = Mock()
        mock_load = Mock()
        mock_filter = Mock()
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.load.return_value = mock_load
        mock_load.filter.return_value = mock_filter
        
        # Mock transform
        mock_transformed = Mock()
        handler._transform_autoloader_data = Mock(return_value=mock_transformed)
        
        # Mock existing files
        mock_table = Mock()
        mock_select = Mock()
        mock_distinct = Mock()
        mock_spark.table.return_value = mock_table
        mock_table.select.return_value = mock_select
        mock_select.distinct.return_value = mock_distinct
        
        # Mock new files
        mock_new_files = Mock()
        mock_new_files.count.return_value = 5
        mock_new_files.write = Mock()
        mock_transformed.join.return_value = mock_new_files
        
        result = handler.rescan_volume()
        
        assert result.total_files_processed == 5
        assert result.total_files_succeeded == 5
        assert result.run_id.startswith("rescan_")

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_validate_volume_path_success(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test validating volume path when accessible."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock successful file listing
        mock_read = Mock()
        mock_format = Mock()
        mock_files = Mock()
        mock_pdf_files = Mock()
        
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.load.return_value = mock_files
        mock_files.count.return_value = 100
        mock_files.filter.return_value = mock_pdf_files
        mock_pdf_files.count.return_value = 75
        
        result = handler.validate_volume_path()
        
        assert result["valid"] is True
        assert result["path"] == mock_config.source_volume_path
        assert result["total_files"] == 100
        assert result["pdf_files"] == 75
        assert result["accessible"] is True

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_validate_volume_path_error(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test validating volume path when not accessible."""
        mock_ws_client.return_value = mock_workspace_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock failed file listing
        mock_spark.read.format.side_effect = Exception("Path not found")
        
        result = handler.validate_volume_path()
        
        assert result["valid"] is False
        assert result["path"] == mock_config.source_volume_path
        assert "Path not found" in result["error"]
        assert result["accessible"] is False

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_cleanup_checkpoint(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test cleaning up checkpoint location."""
        mock_client = mock_workspace_client
        mock_ws_client.return_value = mock_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        handler.cleanup_checkpoint()
        
        mock_client.dbutils.fs.rm.assert_called_once_with(
            mock_config.checkpoint_path,
            recurse=True
        )

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_get_checkpoint_info_exists(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test getting checkpoint info when it exists."""
        mock_client = mock_workspace_client
        mock_ws_client.return_value = mock_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock checkpoint exists
        mock_files = [Mock(), Mock(), Mock()]  # 3 files
        mock_client.dbutils.fs.ls.return_value = mock_files
        
        info = handler.get_checkpoint_info()
        
        assert info["path"] == mock_config.checkpoint_path
        assert info["exists"] is True
        assert info["file_count"] == 3

    @patch('databricks_pdf_ocr.handlers.autoloader.WorkspaceClient')
    @patch('databricks_pdf_ocr.handlers.autoloader.create_source_table')
    def test_get_checkpoint_info_not_exists(self, mock_create_table, mock_ws_client, mock_spark, mock_config, mock_workspace_client):
        """Test getting checkpoint info when it doesn't exist."""
        mock_client = mock_workspace_client
        mock_ws_client.return_value = mock_client
        handler = AutoloaderHandler(mock_spark, mock_config)
        
        # Mock checkpoint doesn't exist
        mock_client.dbutils.fs.ls.side_effect = Exception("Directory not found")
        
        info = handler.get_checkpoint_info()
        
        assert info["path"] == mock_config.checkpoint_path
        assert info["exists"] is False
        assert info["file_count"] == 0