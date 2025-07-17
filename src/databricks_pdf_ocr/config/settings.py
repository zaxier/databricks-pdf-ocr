from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class ProcessingMode(StrEnum):
    INCREMENTAL = "incremental"
    REPROCESS_ALL = "reprocess_all"
    REPROCESS_SPECIFIC = "reprocess_specific"


@dataclass
class ProcessingConfig:
    """Base configuration for all processing operations"""

    catalog: str
    schema: str
    max_retries: int = 3
    retry_delay_seconds: int = 60

    @property
    def base_path(self) -> str:
        """Base path for all tables in the catalog"""
        return f"{self.catalog}.{self.schema}"

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary"""
        return asdict(self)


@dataclass
class AutoloaderConfig(ProcessingConfig):
    """Configuration for Autoloader streaming ingestion"""

    source_volume_path: str = ""
    checkpoint_location: str = ""
    source_table_path: str = ""
    max_file_size_bytes: int | None = None
    file_format: str = "binaryFile"
    # Autoloader-specific configurations
    max_files_per_trigger: int = 1000
    use_notifications: bool = False  
    include_existing_files: bool = True
    backfill_interval: str = "1 day"
    schema_location: str | None = None

    @property
    def base_path(self) -> str:
        """Base path for all tables in the catalog"""
        return f"{self.catalog}.{self.schema}"

    @property
    def source_table_name(self) -> str:
        """Full table name including catalog and schema"""
        return f"{self.base_path}.pdf_source"

    @property
    def checkpoint_path(self) -> str:
        """Full checkpoint location path"""
        return f"{self.checkpoint_location}/pdf_ingestion"

    @property
    def schema_location_path(self) -> str:
        """Schema location for autoloader schema evolution"""
        if self.schema_location:
            return self.schema_location
        return f"{self.checkpoint_location}/schemas"


@dataclass
class OCRConfig(ProcessingConfig):
    """Configuration for OCR processing"""

    target_table_path: str = ""
    state_table_path: str = ""
    max_docs_per_run: int = 100
    max_pages_per_pdf: int | None = None
    processing_mode: ProcessingMode = ProcessingMode.INCREMENTAL
    specific_file_ids: list[str] = field(default_factory=list)
    batch_size: int = 10

    # Claude API configuration
    model_endpoint_name: str = "databricks-claude-3-7-sonnet"
    claude_max_tokens: int = 4096
    claude_temperature: float = 0.0

    # Image processing configuration
    image_max_edge_pixels: int = 1568
    image_dpi: int = 200

    @property
    def base_path(self) -> str:
        """Base path for all tables in the catalog"""
        return f"{self.catalog}.{self.schema}"

    @property
    def target_table_name(self) -> str:
        """Full target table name including catalog and schema"""
        return f"{self.base_path}.pdf_ocr_results"

    @property
    def state_table_name(self) -> str:
        """Full state table name including catalog and schema"""
        return f"{self.base_path}.pdf_processing_state"

    def validate(self) -> None:
        """Validate configuration parameters"""
        if self.max_docs_per_run <= 0:
            raise ValueError("max_docs_per_run must be positive")

        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

        if self.batch_size > self.max_docs_per_run:
            raise ValueError("batch_size cannot exceed max_docs_per_run")

        if self.processing_mode == ProcessingMode.REPROCESS_SPECIFIC and not self.specific_file_ids:
            raise ValueError("specific_file_ids must be provided for REPROCESS_SPECIFIC mode")

        if self.claude_temperature < 0 or self.claude_temperature > 1:
            raise ValueError("claude_temperature must be between 0 and 1")

        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")


@dataclass
class RunMetrics:
    """Metrics for a processing run"""

    run_id: str
    processing_mode: ProcessingMode
    files_processed: int = 0
    files_succeeded: int = 0
    files_failed: int = 0
    total_pages_processed: int = 0
    processing_duration_seconds: float = 0.0
    configuration: dict = field(default_factory=dict)
