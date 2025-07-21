"""Sync integration tests - testing file synchronization with real volumes."""

import shutil
from pathlib import Path

import pytest
from databricks.sdk import WorkspaceClient

from databricks_pdf_ocr.config import SyncConfig
from databricks_pdf_ocr.sync import create_volume_if_not_exists


class TestSyncIntegration:
    """Integration tests for file synchronization with Databricks volumes."""

    @pytest.fixture
    def sync_config(self) -> SyncConfig:
        """Create sync config for testing."""
        return SyncConfig()

    @pytest.mark.integration
    def test_volume_creation(
        self,
        workspace_client: WorkspaceClient,
        sync_config: SyncConfig,
        integration_test_marker: bool,
    ) -> None:
        """Test volume and schema creation functionality."""

        # Test volume creation function
        create_volume_if_not_exists(sync_config)

        # Verify schema exists
        schema_info = workspace_client.schemas.get(f"{sync_config.catalog}.{sync_config.schema}")
        assert schema_info.name == sync_config.schema
        assert schema_info.catalog_name == sync_config.catalog

        # Verify volume exists
        volume_info = workspace_client.volumes.read(
            f"{sync_config.catalog}.{sync_config.schema}.{sync_config.volume}"
        )
        assert volume_info.name == sync_config.volume
        assert volume_info.catalog_name == sync_config.catalog
        assert volume_info.schema_name == sync_config.schema

    @pytest.mark.integration
    def test_sync_with_volsync_library(
        self,
        workspace_client: WorkspaceClient,
        test_volume_setup: dict[str, str],
        test_pdf_files: dict[str, Path],
        temp_local_dir: Path,
        integration_test_marker: bool,
    ) -> None:
        """Test sync functionality using the volsync library."""

        # Copy test files to temporary directory
        copied_files = []
        for file_name, file_path in test_pdf_files.items():
            if file_path.exists():
                dest_path = temp_local_dir / f"sync_test_{file_name}.pdf"
                shutil.copy2(file_path, dest_path)
                copied_files.append(dest_path)

        assert len(copied_files) > 0, "Should have test files to sync"

        # Import and configure volsync
        from volsync import SyncConfig as VolSyncConfig
        from volsync import VolumeSync

        sync_config = VolSyncConfig(
            local_path=temp_local_dir,
            volume_path=f"/Volumes/{test_volume_setup['catalog']}/{test_volume_setup['schema']}/{test_volume_setup['volume']}",
            catalog=test_volume_setup["catalog"],
            schema=test_volume_setup["schema"],
            volume=test_volume_setup["volume"],
            patterns=["*.pdf"],
            exclude_patterns=[],
            dry_run=False,
            force_upload=True,
        )

        # Perform sync
        syncer = VolumeSync()
        result = syncer.sync(sync_config)

        # Verify sync results
        assert result.success_count > 0, "Should sync at least one file"
        assert result.failure_count == 0, "Should have no failures"
        assert result.total_bytes_uploaded > 0, "Should upload some data"

        # Verify files exist in volume
        volume_path = f"/Volumes/{test_volume_setup['catalog']}/{test_volume_setup['schema']}/{test_volume_setup['volume']}"

        try:
            files = workspace_client.files.list_directory_contents(volume_path)
            file_names = [f.name for f in files if f.name and f.name.endswith(".pdf")]

            assert len(file_names) >= len(copied_files), "Should have uploaded files in volume"

        except Exception as e:
            pytest.fail(f"Failed to list volume contents: {e}")

    @pytest.mark.integration
    def test_sync_patterns_and_filtering(
        self,
        workspace_client: WorkspaceClient,
        test_volume_setup: dict[str, str],
        temp_local_dir: Path,
        integration_test_marker: bool,
    ) -> None:
        """Test sync pattern matching and file filtering."""

        # Create test files with different extensions
        test_files = [
            ("document.pdf", b"PDF content"),
            ("image.jpg", b"JPEG content"),
            ("text.txt", b"Text content"),
            ("Document.PDF", b"PDF content uppercase"),
            ("draft_document.pdf", b"Draft PDF content"),
        ]

        for file_name, content in test_files:
            file_path = temp_local_dir / file_name
            with open(file_path, "wb") as f:
                f.write(content)

        from volsync import SyncConfig as VolSyncConfig
        from volsync import VolumeSync

        # Test PDF pattern matching - use case-insensitive approach
        sync_config = VolSyncConfig(
            local_path=temp_local_dir,
            volume_path=f"/Volumes/{test_volume_setup['catalog']}/{test_volume_setup['schema']}/{test_volume_setup['volume']}",
            catalog=test_volume_setup["catalog"],
            schema=test_volume_setup["schema"],
            volume=test_volume_setup["volume"],
            patterns=["*.pdf"],  # Just use lowercase pattern
            exclude_patterns=["*draft*"],
            dry_run=False,
            force_upload=True,
        )

        syncer = VolumeSync()
        result = syncer.sync(sync_config)

        # Should sync 1 PDF file (document.pdf)
        # Note: volsync only tracks files that match patterns, so non-matching files aren't counted as skipped
        assert result.success_count == 1, "Should sync 1 PDF file (document.pdf only)"
        assert len(result.failed_files) == 0, "Should have no failed files"

        # Only document.pdf should be processed (matches *.pdf and not excluded)
        # Other files are outside the scope: image.jpg, text.txt (don't match *.pdf)
        # Document.PDF (case-sensitive, doesn't match *.pdf), draft_document.pdf (excluded by *draft*)
        assert result.uploaded_files == [str(temp_local_dir / "document.pdf")], (
            "Only document.pdf should be uploaded"
        )

    @pytest.mark.integration
    def test_sync_dry_run_mode(
        self,
        test_volume_setup: dict[str, str],
        temp_local_dir: Path,
        integration_test_marker: bool,
    ) -> None:
        """Test sync dry run mode (no actual uploads)."""

        # Create test file
        test_file = temp_local_dir / "dry_run_test.pdf"
        test_content = b"Dry run test content"
        with open(test_file, "wb") as f:
            f.write(test_content)

        from volsync import SyncConfig as VolSyncConfig
        from volsync import VolumeSync

        sync_config = VolSyncConfig(
            local_path=temp_local_dir,
            volume_path=f"/Volumes/{test_volume_setup['catalog']}/{test_volume_setup['schema']}/{test_volume_setup['volume']}",
            catalog=test_volume_setup["catalog"],
            schema=test_volume_setup["schema"],
            volume=test_volume_setup["volume"],
            patterns=["*.pdf"],
            exclude_patterns=[],
            dry_run=True,  # Dry run mode
            force_upload=False,
        )

        syncer = VolumeSync()
        result = syncer.sync(sync_config)

        # In dry run mode, files should be "processed" but not actually uploaded
        assert result.total_bytes_uploaded == 0, "Should not upload any bytes in dry run"
        # Note: volsync may still report success_count > 0 for dry run simulation

    @pytest.mark.integration
    def test_sync_force_upload_behavior(
        self,
        workspace_client: WorkspaceClient,
        test_volume_setup: dict[str, str],
        temp_local_dir: Path,
        integration_test_marker: bool,
    ) -> None:
        """Test force upload behavior vs incremental sync."""

        # Create test file
        test_file = temp_local_dir / "force_test.pdf"
        original_content = b"Original content"
        with open(test_file, "wb") as f:
            f.write(original_content)

        from volsync import SyncConfig as VolSyncConfig
        from volsync import VolumeSync

        # First sync with force=True
        sync_config = VolSyncConfig(
            local_path=temp_local_dir,
            volume_path=f"/Volumes/{test_volume_setup['catalog']}/{test_volume_setup['schema']}/{test_volume_setup['volume']}",
            catalog=test_volume_setup["catalog"],
            schema=test_volume_setup["schema"],
            volume=test_volume_setup["volume"],
            patterns=["*.pdf"],
            exclude_patterns=[],
            dry_run=False,
            force_upload=True,
        )

        syncer = VolumeSync()
        result1 = syncer.sync(sync_config)

        assert result1.success_count > 0, "First sync should upload file"

        # Second sync without force (should skip existing files)
        sync_config.force_upload = False
        result2 = syncer.sync(sync_config)

        # File should be skipped since it already exists and hasn't changed
        assert result2.skip_count > 0, "Second sync should skip existing file"

    @pytest.mark.integration
    def test_sync_error_handling(
        self,
        test_volume_setup: dict[str, str],
        temp_local_dir: Path,
        integration_test_marker: bool,
    ) -> None:
        """Test sync error handling for various failure scenarios."""

        # Create test file
        test_file = temp_local_dir / "error_test.pdf"
        with open(test_file, "wb") as f:
            f.write(b"Test content")

        from volsync import SyncConfig as VolSyncConfig
        from volsync import VolumeSync

        # Test with invalid volume path
        invalid_sync_config = VolSyncConfig(
            local_path=temp_local_dir,
            volume_path="/Volumes/invalid/invalid/invalid",
            catalog="invalid_catalog",
            schema="invalid_schema",
            volume="invalid_volume",
            patterns=["*.pdf"],
            exclude_patterns=[],
            dry_run=False,
            force_upload=True,
        )

        syncer = VolumeSync()

        # This should handle errors gracefully and return failure results
        result = syncer.sync(invalid_sync_config)

        # Should have failures due to invalid volume path
        assert result.failure_count > 0, "Should have failures for invalid volume path"
        assert result.success_count == 0, "Should not succeed with invalid volume path"

    @pytest.mark.integration
    def test_large_file_sync(
        self,
        workspace_client: WorkspaceClient,
        test_volume_setup: dict[str, str],
        temp_local_dir: Path,
        integration_test_marker: bool,
    ) -> None:
        """Test syncing larger files to verify performance and reliability."""

        # Create a larger test file (1MB)
        large_file = temp_local_dir / "large_test.pdf"
        large_content = b"A" * (1024 * 1024)  # 1MB of 'A's

        with open(large_file, "wb") as f:
            f.write(large_content)

        from volsync import SyncConfig as VolSyncConfig
        from volsync import VolumeSync

        sync_config = VolSyncConfig(
            local_path=temp_local_dir,
            volume_path=f"/Volumes/{test_volume_setup['catalog']}/{test_volume_setup['schema']}/{test_volume_setup['volume']}",
            catalog=test_volume_setup["catalog"],
            schema=test_volume_setup["schema"],
            volume=test_volume_setup["volume"],
            patterns=["*.pdf"],
            exclude_patterns=[],
            dry_run=False,
            force_upload=True,
        )

        syncer = VolumeSync()
        result = syncer.sync(sync_config)

        # Verify large file was synced successfully
        assert result.success_count > 0, "Large file should sync successfully"
        assert result.total_bytes_uploaded >= len(large_content), (
            "Should upload at least file size in bytes"
        )

        # Verify file exists and has correct size in volume
        volume_path = f"/Volumes/{test_volume_setup['catalog']}/{test_volume_setup['schema']}/{test_volume_setup['volume']}"

        try:
            files = workspace_client.files.list_directory_contents(volume_path)
            # Look for files that start with "large_test" (volsync adds hash suffix)
            large_files = [f for f in files if f.path and "large_test" in f.path.split("/")[-1]]

            assert len(large_files) > 0, "Large file should exist in volume (with hash suffix)"
            assert large_files[0].file_size and large_files[0].file_size == len(large_content), (
                "File size should match"
            )

        except Exception as e:
            pytest.fail(f"Failed to verify large file: {e}")
