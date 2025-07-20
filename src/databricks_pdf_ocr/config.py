"""Configuration management using dynaconf."""

import logging
from pathlib import Path

from dynaconf import Dynaconf

logger = logging.getLogger(__name__)

# Find project root
project_root = Path(__file__).parent.parent.parent

settings = Dynaconf(
    envvar_prefix="PDF_OCR",
    settings_files=[
        str(project_root / "settings.toml"),
    ],
    load_dotenv=True,
    dotenv_path=str(project_root / ".env"),
    environments=True,
    env_switcher="PDF_OCR_ENV",
)


class AutoloaderConfig:
    """Configuration for autoloader ingestion."""

    def __init__(self):
        self.source_volume_path = str(settings.autoloader.source_volume_path)  # type: ignore
        self.checkpoint_location = str(settings.autoloader.checkpoint_location)  # type: ignore
        self.source_table_path = str(settings.autoloader.source_table_path)  # type: ignore

    @property
    def max_files_per_trigger(self) -> int:
        return int(getattr(settings.autoloader, 'max_files_per_trigger', 100))  # type: ignore

    @property
    def checkpoint_volume_info(self) -> dict:
        """Parse checkpoint_location to extract catalog, schema, and volume."""
        # Example: /Volumes/zaxier_dev/pdf_ocr2/checkpoints/pdf_ingestion
        path_parts = self.checkpoint_location.strip('/').split('/')

        if len(path_parts) >= 4 and path_parts[0] == 'Volumes':
            return {
                'catalog': path_parts[1],
                'schema': path_parts[2],
                'volume': path_parts[3]
            }
        else:
            raise ValueError(f"Invalid checkpoint_location format: {self.checkpoint_location}")


class OCRProcessingConfig:
    """Configuration for OCR processing."""

    def __init__(self):
        self.source_table_path = str(settings.ocr_processing.source_table_path)  # type: ignore
        self.target_table_path = str(settings.ocr_processing.target_table_path)  # type: ignore
        self.state_table_path = str(settings.ocr_processing.state_table_path)  # type: ignore
        self.max_docs_per_run = int(settings.ocr_processing.max_docs_per_run)  # type: ignore
        self.processing_mode = str(settings.ocr_processing.processing_mode)  # type: ignore
        self.batch_size = int(settings.ocr_processing.batch_size)  # type: ignore
        self.max_retries = int(settings.ocr_processing.max_retries)  # type: ignore
        self.retry_delay_seconds = float(settings.ocr_processing.retry_delay_seconds)  # type: ignore

    @property
    def max_pages_per_pdf(self) -> int | None:
        return getattr(settings.ocr_processing, 'max_pages_per_pdf', None)  # type: ignore

    @property
    def specific_file_ids(self) -> list[str]:
        return getattr(settings.ocr_processing, 'specific_file_ids', [])  # type: ignore


class SyncConfig:
    """Configuration for file synchronization."""

    def __init__(self):
        # Extract catalog and schema from source_volume_path
        volume_path = str(settings.autoloader.source_volume_path)  # type: ignore
        path_parts = volume_path.strip('/').split('/')

        if len(path_parts) >= 4 and path_parts[0] == 'Volumes':
            self.catalog = path_parts[1]
            self.schema = path_parts[2]
            self.volume = path_parts[3]
        else:
            raise ValueError(f"Invalid source_volume_path format: {volume_path}")

        self.local_path = str(getattr(settings.sync, 'local_path', 'data/'))
        self.patterns = list(getattr(settings.sync, 'patterns', ['*.pdf']))
        self.exclude_patterns = list(getattr(settings.sync, 'exclude_patterns', []))


class ClaudeConfig:
    """Configuration for Claude API."""

    def __init__(self):
        self.max_tokens = int(settings.claude.claude_max_tokens)  # type: ignore
        self.temperature = float(settings.claude.claude_temperature)  # type: ignore
        self.image_max_edge_pixels = int(settings.claude.image_max_edge_pixels)  # type: ignore
        self.image_dpi = int(settings.claude.image_dpi)  # type: ignore

    @property
    def endpoint_name(self) -> str:
        return str(getattr(settings.claude, 'endpoint_name', 'databricks-claude-3-7-sonnet'))  # type: ignore


class DatabricksConfig:
    """Configuration for Databricks connection."""

    @property
    def host(self) -> str:
        return str(settings.get('DATABRICKS_HOST', ''))  # type: ignore

    @property
    def token(self) -> str:
        return str(settings.get('DATABRICKS_ACCESS_TOKEN', ''))  # type: ignore


def create_spark_session():
    """Create a Spark session using Databricks Connect."""
    try:
        from databricks.connect import DatabricksSession
        session = DatabricksSession.builder.getOrCreate()
        logger.debug("Created standard Databricks Spark session.")
        return session
    except Exception as ex:
        logger.debug(
            "Failed to create standard session due to: %s. Falling back to serverless Spark session.",
            ex,
        )
        try:
            from databricks.connect import DatabricksSession
            session = DatabricksSession.builder.serverless().getOrCreate()
            logger.debug("Using Databricks serverless Spark session.")
            return session
        except Exception as serverless_ex:
            raise RuntimeError(f"Failed to create Databricks session. Standard: {ex}, Serverless: {serverless_ex}")
