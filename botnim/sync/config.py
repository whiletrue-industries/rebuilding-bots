"""
Source Configuration and Versioning Schema for Automated Sync System.

This module defines the unified configuration format for all content sources
(HTML, PDF, spreadsheet) with built-in versioning capabilities.
"""

import hashlib
import json
import yaml
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from urllib.parse import urlparse
from pydantic import BaseModel, Field, field_validator, model_validator
import requests


class SourceType(str, Enum):
    """Supported content source types."""
    HTML = "html"
    PDF = "pdf"
    SPREADSHEET = "spreadsheet"
    WIKISOURCE = "wikisource"


class VersioningStrategy(str, Enum):
    """Available versioning strategies for content sources."""
    HASH = "hash"  # Content hash (SHA-256)
    TIMESTAMP = "timestamp"  # Last modified timestamp
    ETAG = "etag"  # HTTP ETag header
    VERSION_STRING = "version_string"  # Explicit version string
    COMBINED = "combined"  # Hash + timestamp


class FetchStrategy(str, Enum):
    """How to fetch content from the source."""
    DIRECT = "direct"  # Direct HTTP GET
    INDEX_PAGE = "index_page"  # Parse index page for links
    API = "api"  # Use API endpoint
    ASYNC = "async"  # Fetch asynchronously (for large sources)


class HTMLSourceConfig(BaseModel):
    """Configuration for HTML content sources."""
    url: str = Field(..., description="Source URL")
    selector: Optional[str] = Field(None, description="CSS selector for content extraction")
    encoding: Optional[str] = Field(None, description="Content encoding (auto-detected if not specified)")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    retry_attempts: int = Field(default=3, description="Number of retry attempts")
    
    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        """Validate URL format."""
        try:
            result = urlparse(v)
            if not all([result.scheme, result.netloc]):
                raise ValueError('Invalid URL format')
            return v
        except Exception:
            raise ValueError('Invalid URL format')


class PDFProcessingField(BaseModel):
    """Configuration for a PDF processing field."""
    name: str = Field(..., description="Field name")
    type: str = Field(..., description="Field type (string, date, text, array)")
    description: str = Field(..., description="Field description")
    required: bool = Field(default=False, description="Whether field is required")


class PDFProcessingOptions(BaseModel):
    """Configuration for PDF processing options."""
    enable_ocr: bool = Field(default=True, description="Enable OCR for image-based PDFs")
    ocr_language: str = Field(default="heb+eng", description="OCR language")
    chunk_size: int = Field(default=1000, description="Text chunk size")
    chunk_overlap: int = Field(default=200, description="Text chunk overlap")
    max_file_size_mb: int = Field(default=50, description="Maximum file size in MB")


class PDFProcessingConfig(BaseModel):
    """Configuration for PDF processing."""
    model: str = Field(default="gpt-4o-mini", description="OpenAI model to use")
    max_tokens: int = Field(default=4000, description="Maximum tokens for processing")
    temperature: float = Field(default=0.1, description="Processing temperature")
    fields: List[PDFProcessingField] = Field(default_factory=list, description="Fields to extract")
    options: PDFProcessingOptions = Field(default_factory=PDFProcessingOptions, description="Processing options")


class PDFSourceConfig(BaseModel):
    """Configuration for PDF content sources."""
    url: str = Field(..., description="Source URL or index page URL")
    is_index_page: bool = Field(default=False, description="Whether URL is an index page")
    file_pattern: Optional[str] = Field(None, description="File pattern for PDF files")
    download_directory: Optional[str] = Field(None, description="Directory to store downloaded PDFs")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    timeout: int = Field(default=60, description="Request timeout in seconds")
    processing: Optional[PDFProcessingConfig] = Field(None, description="PDF processing configuration")
    
    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        """Validate URL format."""
        try:
            result = urlparse(v)
            if not all([result.scheme, result.netloc]):
                raise ValueError('Invalid URL format')
            return v
        except Exception:
            raise ValueError('Invalid URL format')


class SpreadsheetSourceConfig(BaseModel):
    """Configuration for spreadsheet content sources."""
    url: str = Field(..., description="Google Sheets URL")
    sheet_name: Optional[str] = Field(None, description="Specific sheet name")
    range: Optional[str] = Field(None, description="Cell range (e.g., 'A1:D100')")
    credentials_path: Optional[str] = Field(None, description="Path to service account credentials")
    use_adc: bool = Field(default=False, description="Use Application Default Credentials")
    
    @field_validator('url')
    @classmethod
    def validate_google_sheets_url(cls, v):
        """Validate Google Sheets URL format."""
        if not v.startswith('https://docs.google.com/spreadsheets/'):
            raise ValueError('Must be a Google Sheets URL')
        return v


class ContentSource(BaseModel):
    """Configuration for a single content source."""
    id: str = Field(..., description="Unique source identifier")
    name: str = Field(..., description="Human-readable source name")
    description: Optional[str] = Field(None, description="Source description")
    type: SourceType = Field(..., description="Source type")
    
    # Source-specific configuration
    html_config: Optional[HTMLSourceConfig] = Field(None, description="HTML source configuration")
    pdf_config: Optional[PDFSourceConfig] = Field(None, description="PDF source configuration")
    spreadsheet_config: Optional[SpreadsheetSourceConfig] = Field(None, description="Spreadsheet source configuration")
    
    # Versioning configuration
    versioning_strategy: VersioningStrategy = Field(default=VersioningStrategy.HASH, description="Versioning strategy")
    version_string: Optional[str] = Field(None, description="Explicit version string (for VERSION_STRING strategy)")
    
    # Fetching configuration
    fetch_strategy: FetchStrategy = Field(default=FetchStrategy.DIRECT, description="Fetch strategy")
    fetch_interval: Optional[int] = Field(None, description="Fetch interval in seconds (for async sources)")
    
    # Processing configuration
    enabled: bool = Field(default=True, description="Whether this source is enabled")
    priority: int = Field(default=1, description="Processing priority (lower = higher priority)")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Source tags for categorization")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    @model_validator(mode='after')
    def validate_source_type_config(self):
        """Validate that source type has corresponding config."""
        if self.type == SourceType.HTML and not self.html_config:
            raise ValueError('HTML source requires html_config')
        elif self.type == SourceType.PDF and not self.pdf_config:
            raise ValueError('PDF source requires pdf_config')
        elif self.type == SourceType.SPREADSHEET and not self.spreadsheet_config:
            raise ValueError('Spreadsheet source requires spreadsheet_config')
        return self


class VersionInfo(BaseModel):
    """Version information for a content source."""
    source_id: str = Field(..., description="Source identifier")
    version_hash: str = Field(..., description="Content hash")
    version_timestamp: datetime = Field(..., description="Version timestamp")
    version_string: Optional[str] = Field(None, description="Version string")
    etag: Optional[str] = Field(None, description="HTTP ETag")
    content_size: int = Field(..., description="Content size in bytes")
    last_fetch: datetime = Field(..., description="Last fetch timestamp")
    fetch_status: str = Field(..., description="Fetch status (success/error)")
    error_message: Optional[str] = Field(None, description="Error message if fetch failed")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "source_id": self.source_id,
            "version_hash": self.version_hash,
            "version_timestamp": self.version_timestamp.isoformat(),
            "version_string": self.version_string,
            "etag": self.etag,
            "content_size": self.content_size,
            "last_fetch": self.last_fetch.isoformat(),
            "fetch_status": self.fetch_status,
            "error_message": self.error_message
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VersionInfo':
        """Create from dictionary."""
        # Convert ISO timestamps back to datetime objects
        if 'version_timestamp' in data:
            data['version_timestamp'] = datetime.fromisoformat(data['version_timestamp'])
        if 'last_fetch' in data:
            data['last_fetch'] = datetime.fromisoformat(data['last_fetch'])
        return cls(**data)


class SyncConfig(BaseModel):
    """Main configuration for the automated sync system."""
    version: str = Field(default="1.0.0", description="Configuration version")
    name: str = Field(..., description="Configuration name")
    description: Optional[str] = Field(None, description="Configuration description")
    
    # Sources configuration
    sources: List[ContentSource] = Field(..., description="List of content sources")
    
    # Global settings
    default_versioning_strategy: VersioningStrategy = Field(
        default=VersioningStrategy.HASH, 
        description="Default versioning strategy"
    )
    default_fetch_strategy: FetchStrategy = Field(
        default=FetchStrategy.DIRECT, 
        description="Default fetch strategy"
    )
    
    # Storage configuration
    cache_directory: str = Field(default="./cache", description="Cache directory path")
    embedding_cache_path: str = Field(default="./cache/embeddings.sqlite", description="Embedding cache path")
    version_cache_path: str = Field(default="./cache/versions.json", description="Version cache path")
    
    # Processing configuration
    max_concurrent_sources: int = Field(default=5, description="Maximum concurrent source processing")
    timeout_per_source: int = Field(default=300, description="Timeout per source in seconds")
    
    # Logging configuration
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: Optional[str] = Field(None, description="Log file path")
    
    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> 'SyncConfig':
        """Load configuration from YAML file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        return cls(**data)
    
    def to_yaml(self, path: Union[str, Path]) -> None:
        """Save configuration to YAML file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to dict with enum values as strings
        data = self.model_dump(mode='json')
        
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
    
    def get_source_by_id(self, source_id: str) -> Optional[ContentSource]:
        """Get source configuration by ID."""
        for source in self.sources:
            if source.id == source_id:
                return source
        return None
    
    def get_sources_by_type(self, source_type: SourceType) -> List[ContentSource]:
        """Get all sources of a specific type."""
        return [source for source in self.sources if source.type == source_type]
    
    def get_enabled_sources(self) -> List[ContentSource]:
        """Get all enabled sources."""
        return [source for source in self.sources if source.enabled]


class VersionManager:
    """Manages version information for content sources."""
    
    def __init__(self, cache_path: str):
        """Initialize version manager."""
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_versions()
    
    def _load_versions(self) -> None:
        """Load version information from cache."""
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.versions = {k: VersionInfo.from_dict(v) for k, v in data.items()}
            except Exception as e:
                print(f"Warning: Failed to load version cache: {e}")
                self.versions = {}
        else:
            self.versions = {}
    
    def _save_versions(self) -> None:
        """Save version information to cache."""
        data = {k: v.to_dict() for k, v in self.versions.items()}
        with open(self.cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def get_version(self, source_id: str) -> Optional[VersionInfo]:
        """Get version information for a source."""
        return self.versions.get(source_id)
    
    def update_version(self, version_info: VersionInfo) -> None:
        """Update version information for a source."""
        self.versions[version_info.source_id] = version_info
        self._save_versions()
    
    def has_changed(self, source_id: str, new_hash: str, new_timestamp: Optional[datetime] = None) -> bool:
        """Check if source content has changed."""
        current_version = self.get_version(source_id)
        if not current_version:
            return True
        
        # Compare hash
        if current_version.version_hash != new_hash:
            return True
        
        # Compare timestamp if provided
        if new_timestamp and current_version.version_timestamp != new_timestamp:
            return True
        
        return False
    
    def compute_content_hash(self, content: Union[str, bytes]) -> str:
        """Compute SHA-256 hash of content."""
        if isinstance(content, str):
            content = content.encode('utf-8')
        return hashlib.sha256(content).hexdigest()
    
    def get_fetch_timestamp(self, url: str) -> Optional[datetime]:
        """Get last modified timestamp from HTTP headers."""
        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                last_modified = response.headers.get('Last-Modified')
                if last_modified:
                    # Parse RFC 2822 date format
                    from email.utils import parsedate_to_datetime
                    return parsedate_to_datetime(last_modified)
        except Exception:
            pass
        return None


def create_example_config() -> SyncConfig:
    """Create an example configuration for testing."""
    return SyncConfig(
        name="Example Sync Configuration",
        description="Example configuration with all source types",
        sources=[
            ContentSource(
                id="knesset-laws-html",
                name="Knesset Laws (HTML)",
                description="HTML version of Knesset laws",
                type=SourceType.HTML,
                html_config=HTMLSourceConfig(
                    url="https://main.knesset.gov.il/Activity/Legislation/Pages/default.aspx"
                ),
                versioning_strategy=VersioningStrategy.HASH,
                tags=["legal", "knesset", "laws"]
            ),
            ContentSource(
                id="ethics-decisions-pdf",
                name="Ethics Committee Decisions (PDF)",
                description="PDF decisions from ethics committee",
                type=SourceType.PDF,
                pdf_config=PDFSourceConfig(
                    url="https://main.knesset.gov.il/Activity/Committees/Ethics/Pages/default.aspx",
                    is_index_page=True
                ),
                versioning_strategy=VersioningStrategy.COMBINED,
                fetch_strategy=FetchStrategy.INDEX_PAGE,
                tags=["legal", "ethics", "decisions"]
            ),
            ContentSource(
                id="spreadsheet-data",
                name="Spreadsheet Data",
                description="Google Sheets data",
                type=SourceType.SPREADSHEET,
                spreadsheet_config=SpreadsheetSourceConfig(
                    url="https://docs.google.com/spreadsheets/d/1fEgiCLNMQQZqBgQFlkABXgke8I2kI1i1XUvj8Yba9Ow/edit"
                ),
                versioning_strategy=VersioningStrategy.TIMESTAMP,
                fetch_strategy=FetchStrategy.ASYNC,
                tags=["data", "spreadsheet"]
            )
        ]
    )


if __name__ == "__main__":
    # Create and save example configuration
    config = create_example_config()
    config.to_yaml("example_sync_config.yaml")
    print("Example configuration saved to example_sync_config.yaml") 