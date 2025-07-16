"""PDF Sync implementation for syncing local PDFs to Databricks volumes."""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


@dataclass
class SyncConfig:
    """Configuration for PDF sync operations."""

    local_path: Path
    volume_path: str
    catalog: str
    schema: str
    volume: str
    patterns: list[str] = field(default_factory=lambda: ["*.pdf", "*.PDF"])
    exclude_patterns: list[str] = field(default_factory=list)
    dry_run: bool = False
    force_upload: bool = False
    max_concurrent_uploads: int = 5


@dataclass
class FileMetadata:
    """Metadata for a file."""

    path: Path
    size: int
    modified_time: float
    hash: str


@dataclass
class SyncResult:
    """Result of a sync operation."""

    uploaded_files: list[str] = field(default_factory=list)
    skipped_files: list[str] = field(default_factory=list)
    failed_files: list[tuple[str, str]] = field(default_factory=list)
    total_bytes_uploaded: int = 0
    duration_seconds: float = 0.0

    @property
    def success_count(self) -> int:
        return len(self.uploaded_files)

    @property
    def skip_count(self) -> int:
        return len(self.skipped_files)

    @property
    def failure_count(self) -> int:
        return len(self.failed_files)

    @property
    def total_count(self) -> int:
        return self.success_count + self.skip_count + self.failure_count


class PDFSync:
    """Syncs PDF files from local directory to Databricks volume."""

    def __init__(self, workspace_client: WorkspaceClient | None = None):
        """Initialize PDFSync with optional workspace client."""
        self.workspace_client = workspace_client or WorkspaceClient()
        self._file_cache: dict[str, FileMetadata] = {}

    def sync(self, config: SyncConfig) -> SyncResult:
        """Sync PDFs from local directory to Databricks volume."""
        start_time = datetime.now()
        result = SyncResult()

        logger.info(f"Starting sync from {config.local_path} to {config.volume_path}")

        # Validate paths
        if not config.local_path.exists():
            raise ValueError(f"Local path does not exist: {config.local_path}")

        if not config.local_path.is_dir():
            raise ValueError(f"Local path is not a directory: {config.local_path}")

        # Get local files
        local_files = self._get_local_files(config)
        logger.info(f"Found {len(local_files)} PDF files to process")

        # Get remote files
        remote_files = self._get_remote_files(config) if not config.force_upload else {}
        logger.info(f"Found {len(remote_files)} existing files in volume")

        # Determine files to upload
        files_to_upload = self._determine_files_to_upload(local_files, remote_files, config)

        if config.dry_run:
            logger.info("DRY RUN: No files will be uploaded")
            for file_path in files_to_upload:
                result.uploaded_files.append(str(file_path))
        else:
            # Upload files
            for file_path, metadata in files_to_upload.items():
                try:
                    self._upload_file(file_path, metadata, config)
                    result.uploaded_files.append(str(file_path))
                    result.total_bytes_uploaded += metadata.size
                except Exception as e:
                    logger.error(f"Failed to upload {file_path}: {e}")
                    result.failed_files.append((str(file_path), str(e)))

        # Record skipped files
        for file_path in local_files:
            if file_path not in files_to_upload:
                result.skipped_files.append(str(file_path))

        result.duration_seconds = (datetime.now() - start_time).total_seconds()

        self._log_summary(result)

        return result

    def _get_local_files(self, config: SyncConfig) -> dict[Path, FileMetadata]:
        """Get all PDF files from local directory."""
        files = {}

        for pattern in config.patterns:
            for file_path in config.local_path.rglob(pattern):
                if file_path.is_file() and not self._is_excluded(file_path, config):
                    metadata = self._get_file_metadata(file_path)
                    files[file_path] = metadata

        return files

    def _get_remote_files(self, config: SyncConfig) -> dict[str, str]:
        """Get existing files from Databricks volume."""
        remote_files = {}

        try:
            volume_path = f"/Volumes/{config.catalog}/{config.schema}/{config.volume}"
            for file_info in self.workspace_client.files.list_directory_contents(
                directory_path=volume_path
            ):
                if file_info.path and file_info.path.lower().endswith(".pdf"):
                    # Extract relative path and hash from filename if available
                    filename = Path(file_info.path).name
                    if "_" in filename and filename.count("_") >= 1:
                        # Assuming format: originalname_hash.pdf
                        parts = filename.rsplit("_", 1)
                        if len(parts) == 2 and parts[1].endswith(".pdf"):
                            hash_value = parts[1][:-4]  # Remove .pdf
                            remote_files[file_info.path] = hash_value
        except Exception as e:
            logger.warning(f"Failed to list remote files: {e}")

        return remote_files

    def _determine_files_to_upload(
        self,
        local_files: dict[Path, FileMetadata],
        remote_files: dict[str, str],
        config: SyncConfig,
    ) -> dict[Path, FileMetadata]:
        """Determine which files need to be uploaded."""
        files_to_upload = {}

        for file_path, metadata in local_files.items():
            relative_path = file_path.relative_to(config.local_path)

            # Check if file exists in remote with same hash
            needs_upload = True
            if not config.force_upload:
                for _, remote_hash in remote_files.items():
                    if remote_hash == metadata.hash:
                        needs_upload = False
                        logger.debug(f"File already exists with same hash: {relative_path}")
                        break

            if needs_upload:
                files_to_upload[file_path] = metadata

        return files_to_upload

    def _upload_file(self, file_path: Path, metadata: FileMetadata, config: SyncConfig) -> None:
        """Upload a single file to Databricks volume."""
        relative_path = file_path.relative_to(config.local_path)

        # Create target filename with hash
        target_filename = f"{file_path.stem}_{metadata.hash}{file_path.suffix}"
        target_path = f"/Volumes/{config.catalog}/{config.schema}/{config.volume}/{target_filename}"

        logger.info(f"Uploading {relative_path} to {target_path}")

        with open(file_path, "rb") as f:
            self.workspace_client.files.upload(file_path=target_path, contents=f, overwrite=True)

    def _get_file_metadata(self, file_path: Path) -> FileMetadata:
        """Get metadata for a file including SHA-256 hash."""
        # Check cache first
        cache_key = str(file_path)
        stat = file_path.stat()

        if cache_key in self._file_cache:
            cached = self._file_cache[cache_key]
            if cached.modified_time == stat.st_mtime and cached.size == stat.st_size:
                return cached

        # Calculate hash
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)

        metadata = FileMetadata(
            path=file_path,
            size=stat.st_size,
            modified_time=stat.st_mtime,
            hash=hash_sha256.hexdigest(),
        )

        # Update cache
        self._file_cache[cache_key] = metadata

        return metadata

    def _is_excluded(self, file_path: Path, config: SyncConfig) -> bool:
        """Check if file matches any exclude pattern."""
        for pattern in config.exclude_patterns:
            if file_path.match(pattern):
                return True
        return False

    def _log_summary(self, result: SyncResult) -> None:
        """Log summary of sync operation."""
        logger.info(
            f"Sync completed in {result.duration_seconds:.1f}s: "
            f"{result.success_count} uploaded, "
            f"{result.skip_count} skipped, "
            f"{result.failure_count} failed"
        )

        if result.total_bytes_uploaded > 0:
            mb_uploaded = result.total_bytes_uploaded / (1024 * 1024)
            logger.info(f"Total data uploaded: {mb_uploaded:.2f} MB")

        if result.failed_files:
            logger.error("Failed uploads:")
            for file_path, error in result.failed_files:
                logger.error(f"  - {file_path}: {error}")
