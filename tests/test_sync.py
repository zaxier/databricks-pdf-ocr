"""Unit tests for PDF sync functionality."""

import hashlib
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from databricks_pdf_ocr.sync import PDFSync, SyncConfig, SyncResult


class TestPDFSync:
    """Test cases for PDFSync class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory with test PDFs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            # Create test PDF files
            (tmp_path / "test1.pdf").write_bytes(b"PDF content 1")
            (tmp_path / "test2.PDF").write_bytes(b"PDF content 2")
            (tmp_path / "subdir").mkdir()
            (tmp_path / "subdir" / "test3.pdf").write_bytes(b"PDF content 3")
            (tmp_path / "exclude_me.pdf").write_bytes(b"Excluded PDF")
            (tmp_path / "not_pdf.txt").write_bytes(b"Not a PDF")

            yield tmp_path

    @pytest.fixture
    def mock_workspace_client(self):
        """Create a mock workspace client."""
        client = Mock()
        client.files = Mock()
        client.volumes = Mock()
        return client

    @pytest.fixture
    def sync_config(self, temp_dir):
        """Create a test sync configuration."""
        return SyncConfig(
            local_path=temp_dir,
            volume_path="/Volumes/test/test/test",
            catalog="test",
            schema="test",
            volume="test",
            patterns=["*.pdf", "*.PDF"],
            exclude_patterns=["*exclude*"],
        )

    def test_get_local_files(self, temp_dir, sync_config, mock_workspace_client):
        """Test getting local PDF files."""
        sync = PDFSync(mock_workspace_client)
        files = sync._get_local_files(sync_config)

        # Should find 3 files (test1.pdf, test2.PDF, subdir/test3.pdf)
        # exclude_me.pdf should be excluded
        assert len(files) == 3

        # Check file paths
        file_names = {f.name for f in files.keys()}
        assert "test1.pdf" in file_names
        assert "test2.PDF" in file_names
        assert "test3.pdf" in file_names
        assert "exclude_me.pdf" not in file_names
        assert "not_pdf.txt" not in file_names

    def test_get_file_metadata(self, temp_dir, mock_workspace_client):
        """Test file metadata extraction."""
        sync = PDFSync(mock_workspace_client)
        test_file = temp_dir / "test1.pdf"

        metadata = sync._get_file_metadata(test_file)

        assert metadata.path == test_file
        assert metadata.size == 13  # len(b"PDF content 1")
        assert metadata.modified_time > 0

        # Check hash
        expected_hash = hashlib.sha256(b"PDF content 1").hexdigest()
        assert metadata.hash == expected_hash

    def test_file_metadata_caching(self, temp_dir, mock_workspace_client):
        """Test that file metadata is cached."""
        sync = PDFSync(mock_workspace_client)
        test_file = temp_dir / "test1.pdf"

        # Get metadata twice
        metadata1 = sync._get_file_metadata(test_file)
        metadata2 = sync._get_file_metadata(test_file)

        # Should be the same object (cached)
        assert metadata1 is metadata2
        assert len(sync._file_cache) == 1

    def test_is_excluded(self, temp_dir, sync_config, mock_workspace_client):
        """Test file exclusion logic."""
        sync = PDFSync(mock_workspace_client)

        assert sync._is_excluded(temp_dir / "exclude_me.pdf", sync_config) is True
        assert sync._is_excluded(temp_dir / "test1.pdf", sync_config) is False
        assert sync._is_excluded(temp_dir / "subdir" / "exclude_test.pdf", sync_config) is True

    def test_determine_files_to_upload_all_new(self, temp_dir, sync_config, mock_workspace_client):
        """Test determining files to upload when all are new."""
        sync = PDFSync(mock_workspace_client)

        local_files = sync._get_local_files(sync_config)
        remote_files = {}  # No remote files

        files_to_upload = sync._determine_files_to_upload(local_files, remote_files, sync_config)

        # All local files should be uploaded
        assert len(files_to_upload) == len(local_files)

    def test_determine_files_to_upload_some_exist(
        self, temp_dir, sync_config, mock_workspace_client
    ):
        """Test determining files to upload when some already exist."""
        sync = PDFSync(mock_workspace_client)

        local_files = sync._get_local_files(sync_config)

        # Simulate one file already exists remotely
        test1_hash = hashlib.sha256(b"PDF content 1").hexdigest()
        remote_files = {f"/Volumes/test/test/test/test1_{test1_hash}.pdf": test1_hash}

        files_to_upload = sync._determine_files_to_upload(local_files, remote_files, sync_config)

        # Only 2 files should be uploaded (test2.PDF and test3.pdf)
        assert len(files_to_upload) == 2
        file_names = {f.name for f in files_to_upload.keys()}
        assert "test1.pdf" not in file_names
        assert "test2.PDF" in file_names
        assert "test3.pdf" in file_names

    def test_determine_files_to_upload_force(self, temp_dir, sync_config, mock_workspace_client):
        """Test force upload ignores existing files."""
        sync = PDFSync(mock_workspace_client)
        sync_config.force_upload = True

        local_files = sync._get_local_files(sync_config)

        # Simulate all files exist remotely
        remote_files = {"dummy": "hash"}

        files_to_upload = sync._determine_files_to_upload(local_files, remote_files, sync_config)

        # All files should be uploaded with force
        assert len(files_to_upload) == len(local_files)

    @patch("databricks_pdf_ocr.sync.pdf_sync.logger")
    def test_sync_dry_run(self, mock_logger, temp_dir, sync_config, mock_workspace_client):
        """Test sync in dry run mode."""
        sync = PDFSync(mock_workspace_client)
        sync_config.dry_run = True

        # Mock remote files listing
        mock_workspace_client.files.list_directory_contents.return_value = []

        result = sync.sync(sync_config)

        # Check results
        assert result.success_count == 3  # All files "uploaded" in dry run
        assert result.skip_count == 0
        assert result.failure_count == 0
        assert result.total_bytes_uploaded == 0  # No actual upload in dry run

        # Verify no actual upload was attempted
        mock_workspace_client.files.upload.assert_not_called()

    @patch("databricks_pdf_ocr.sync.pdf_sync.logger")
    def test_sync_with_upload(self, mock_logger, temp_dir, sync_config, mock_workspace_client):
        """Test sync with actual upload."""
        sync = PDFSync(mock_workspace_client)

        # Mock remote files listing
        mock_workspace_client.files.list_directory_contents.return_value = []

        result = sync.sync(sync_config)

        # Check results
        assert result.success_count == 3
        assert result.skip_count == 0
        assert result.failure_count == 0
        assert result.total_bytes_uploaded == 39  # 13 + 13 + 13

        # Verify uploads were attempted
        assert mock_workspace_client.files.upload.call_count == 3

    @patch("databricks_pdf_ocr.sync.pdf_sync.logger")
    def test_sync_with_failures(self, mock_logger, temp_dir, sync_config, mock_workspace_client):
        """Test sync with upload failures."""
        sync = PDFSync(mock_workspace_client)

        # Mock remote files listing
        mock_workspace_client.files.list_directory_contents.return_value = []

        # Make first upload fail
        mock_workspace_client.files.upload.side_effect = [Exception("Upload failed"), None, None]

        result = sync.sync(sync_config)

        # Check results
        assert result.success_count == 2
        assert result.skip_count == 0
        assert result.failure_count == 1
        assert len(result.failed_files) == 1
        assert "Upload failed" in result.failed_files[0][1]

    def test_sync_invalid_path(self, sync_config, mock_workspace_client):
        """Test sync with invalid local path."""
        sync = PDFSync(mock_workspace_client)
        sync_config.local_path = Path("/non/existent/path")

        with pytest.raises(ValueError, match="Local path does not exist"):
            sync.sync(sync_config)

    def test_sync_file_not_directory(self, temp_dir, sync_config, mock_workspace_client):
        """Test sync with file instead of directory."""
        sync = PDFSync(mock_workspace_client)
        sync_config.local_path = temp_dir / "test1.pdf"

        with pytest.raises(ValueError, match="Local path is not a directory"):
            sync.sync(sync_config)

    def test_get_remote_files_error_handling(self, sync_config, mock_workspace_client):
        """Test remote files listing error handling."""
        sync = PDFSync(mock_workspace_client)

        # Make listing fail
        mock_workspace_client.files.list_directory_contents.side_effect = Exception("API error")

        # Should return empty dict and not raise
        remote_files = sync._get_remote_files(sync_config)
        assert remote_files == {}


class TestSyncResult:
    """Test cases for SyncResult class."""

    def test_sync_result_properties(self):
        """Test SyncResult computed properties."""
        result = SyncResult()

        result.uploaded_files = ["file1.pdf", "file2.pdf"]
        result.skipped_files = ["file3.pdf"]
        result.failed_files = [("file4.pdf", "Error")]

        assert result.success_count == 2
        assert result.skip_count == 1
        assert result.failure_count == 1
        assert result.total_count == 4
