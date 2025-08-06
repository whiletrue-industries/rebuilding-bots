"""
Sync module for automated, versioned, cloud-native content synchronization.

This module provides infrastructure for periodic synchronization of content sources
(HTML, PDF, spreadsheets) into the vector store, with caching, duplicate detection,
and versioning capabilities.
"""

from .config import (
    SyncConfig, ContentSource, VersionManager, VersionInfo,
    SourceType, VersioningStrategy, FetchStrategy,
    HTMLSourceConfig, PDFSourceConfig, SpreadsheetSourceConfig,
    create_example_config
)

from .cache import (
    SyncCache, CacheEntry, DuplicateInfo, DuplicateDetector
)

from .html_fetcher import (
    HTMLFetcher, HTMLProcessor, fetch_and_parse_html
)

from .embedding_processor import (
    CloudEmbeddingStorage, EmbeddingChangeDetector, BatchEmbeddingProcessor,
    EmbeddingCacheManager, SyncEmbeddingProcessor, EmbeddingInfo, EmbeddingBatch
)

__all__ = [
    # Configuration
    'SyncConfig',
    'ContentSource', 
    'VersionManager',
    'VersionInfo',
    'SourceType',
    'VersioningStrategy',
    'FetchStrategy',
    'HTMLSourceConfig',
    'PDFSourceConfig',
    'SpreadsheetSourceConfig',
    'create_example_config',
    
    # Caching
    'SyncCache',
    'CacheEntry',
    'DuplicateInfo',
    'DuplicateDetector',
    
    # HTML Processing
    'HTMLFetcher',
    'HTMLProcessor',
    'fetch_and_parse_html',
    
    # Embedding Processing
    'CloudEmbeddingStorage',
    'EmbeddingChangeDetector', 
    'BatchEmbeddingProcessor',
    'EmbeddingCacheManager',
    'SyncEmbeddingProcessor',
    'EmbeddingInfo',
    'EmbeddingBatch',
] 