"""Configuration management using dynaconf."""

from dynaconf import Dynaconf
from pathlib import Path
from typing import List, Optional

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
    def max_pages_per_pdf(self) -> Optional[int]:
        return getattr(settings.ocr_processing, 'max_pages_per_pdf', None)  # type: ignore
    
    @property
    def specific_file_ids(self) -> List[str]:
        return getattr(settings.ocr_processing, 'specific_file_ids', [])  # type: ignore


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