# Volume Sync Module

This module provides functionality to sync files from local directories to Databricks volumes.

## Features

- One-way sync from local to Databricks volume
- Hash-based change detection using SHA-256
- Support for multiple file patterns
- Exclude patterns for filtering
- Dry-run mode for testing
- Progress tracking and statistics
- CLI interface for manual operations

## Installation

The volume sync module can be installed as a standalone package. Ensure you have the Databricks SDK installed:

```bash
pip install databricks-sdk
```

## Usage

### Command Line Interface

The sync module provides a CLI with several commands:

#### Upload Files

```bash
# Basic upload
python -m volsync upload /path/to/files \
    --catalog my_catalog \
    --schema my_schema \
    --volume documents

# With options
python -m volsync upload /path/to/files \
    --catalog my_catalog \
    --schema my_schema \
    --volume documents \
    --pattern "*.txt" \
    --pattern "*.json" \
    --exclude "*draft*" \
    --dry-run \
    --verbose
```

#### List Remote Files

```bash
python -m volsync list-remote \
    --catalog my_catalog \
    --schema my_schema \
    --volume documents
```

#### Create Volume

```bash
python -m volsync create-volume \
    --catalog my_catalog \
    --schema my_schema \
    --volume documents
```

### Programmatic Usage

```python
from volsync import VolumeSync, SyncConfig
from pathlib import Path

# Configure sync
config = SyncConfig(
    local_path=Path("/path/to/files"),
    volume_path="/Volumes/catalog/schema/volume",
    catalog="my_catalog",
    schema="my_schema",
    volume="documents",
    patterns=["*.txt", "*.json"],
    exclude_patterns=["*draft*", "*temp*"],
    dry_run=False,
    force_upload=False
)

# Run sync
sync = VolumeSync()
result = sync.sync(config)

# Check results
print(f"Uploaded: {result.success_count} files")
print(f"Skipped: {result.skip_count} files")
print(f"Failed: {result.failure_count} files")
print(f"Total bytes: {result.total_bytes_uploaded}")
```

## Configuration Options

- `local_path`: Path to local directory containing files
- `volume_path`: Full path to Databricks volume
- `catalog`: Databricks catalog name
- `schema`: Databricks schema name
- `volume`: Databricks volume name
- `patterns`: List of file patterns to include (default: ["*.*"])
- `exclude_patterns`: List of patterns to exclude
- `dry_run`: If True, show what would be uploaded without uploading
- `force_upload`: If True, upload all files regardless of existing copies
- `max_concurrent_uploads`: Maximum number of concurrent uploads (default: 5)

## How It Works

1. **File Discovery**: Scans local directory for files matching patterns
2. **Hash Calculation**: Computes SHA-256 hash for each file
3. **Change Detection**: Compares local files with remote files by hash
4. **Upload**: Uploads only new or changed files
5. **Naming**: Remote files are named with format: `originalname_hash.ext`

## Authentication

The sync module uses the Databricks SDK for authentication. Configure authentication using one of these methods:

1. Environment variables:
   ```bash
   databricks auth login --host https://your-workspace.databricks.com
   export DATABRICKS_TOKEN="your-token"
   ```

2. Databricks CLI profile:
   ```bash
   databricks configure --token
   ```

3. Default authentication chain (for Databricks notebooks/jobs)

## Error Handling

- Failed uploads are tracked and reported
- Partial sync results are returned even if some files fail
- Detailed error messages for troubleshooting

## Performance

- Files are hashed once and cached during sync
- Only changed files are uploaded
- Progress is tracked in real-time
- Summary statistics provided after sync