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
        self.source_volume_path = settings.autoloader.source_volume_path
        self.checkpoint_location = settings.autoloader.checkpoint_location
        self.source_table_path = settings.autoloader.source_table_path
        
    @property
    def max_files_per_trigger(self) -> int:
        return getattr(settings.autoloader, 'max_files_per_trigger', 100)


class OCRProcessingConfig:
    """Configuration for OCR processing."""
    
    def __init__(self):
        self.target_table_path = settings.ocr_processing.target_table_path
        self.state_table_path = settings.ocr_processing.state_table_path
        self.max_docs_per_run = settings.ocr_processing.max_docs_per_run
        self.processing_mode = settings.ocr_processing.processing_mode
        self.batch_size = settings.ocr_processing.batch_size
        self.max_retries = settings.ocr_processing.max_retries
        self.retry_delay_seconds = settings.ocr_processing.retry_delay_seconds
        
    @property
    def max_pages_per_pdf(self) -> Optional[int]:
        return getattr(settings.ocr_processing, 'max_pages_per_pdf', None)
    
    @property
    def specific_file_ids(self) -> List[str]:
        return getattr(settings.ocr_processing, 'specific_file_ids', [])


class ClaudeConfig:
    """Configuration for Claude API."""
    
    def __init__(self):
        self.max_tokens = settings.claude.claude_max_tokens
        self.temperature = settings.claude.claude_temperature
        self.image_max_edge_pixels = settings.claude.image_max_edge_pixels
        self.image_dpi = settings.claude.image_dpi
        
    @property
    def endpoint_name(self) -> str:
        return getattr(settings.claude, 'endpoint_name', 'databricks-claude-3-7-sonnet')


class DatabricksConfig:
    """Configuration for Databricks connection."""
    
    @property
    def host(self) -> str:
        return settings.get('DATABRICKS_HOST', '')
        
    @property
    def token(self) -> str:
        return settings.get('DATABRICKS_ACCESS_TOKEN', '')