# Source Configuration and Versioning Schema

## Overview

This document describes the unified configuration format for the automated sync system that handles all content source types (HTML, PDF, spreadsheet) with built-in versioning capabilities.

## Key Features

- **Unified Configuration**: Single YAML format for all source types
- **Versioning Strategies**: Multiple versioning approaches (hash, timestamp, ETag, etc.)
- **Fetch Strategies**: Different fetching methods (direct, index page, async, etc.)
- **Validation**: Comprehensive validation with Pydantic models
- **Extensible**: Easy to add new source types and strategies

## Configuration Structure

### Main Configuration (`SyncConfig`)

```yaml
version: "1.0.0"
name: "Knesset Content Sync Configuration"
description: "Automated sync configuration for Knesset legal content sources"

# Global settings
default_versioning_strategy: "hash"
default_fetch_strategy: "direct"

# Storage configuration
cache_directory: "./cache"
embedding_cache_path: "./cache/embeddings.sqlite"
version_cache_path: "./cache/versions.json"

# Processing configuration
max_concurrent_sources: 5
timeout_per_source: 300

# Logging configuration
log_level: "INFO"
log_file: "./logs/sync.log"

# Content sources
sources:
  # ... source configurations
```

### Source Types

#### 1. HTML Sources

```yaml
- id: "knesset-laws-html"
  name: "Knesset Laws (HTML)"
  description: "HTML version of Knesset laws and regulations"
  type: "html"
  html_config:
    url: "https://main.knesset.gov.il/Activity/Legislation/Pages/default.aspx"
    selector: "#content"
    encoding: "utf-8"
    timeout: 30
    retry_attempts: 3
  versioning_strategy: "hash"
  fetch_strategy: "direct"
  enabled: true
  priority: 1
  tags: ["legal", "knesset", "laws", "html"]
```

#### 2. PDF Sources

```yaml
- id: "ethics-decisions-pdf"
  name: "Ethics Committee Decisions (PDF)"
  description: "PDF decisions from ethics committee"
  type: "pdf"
  pdf_config:
    url: "https://main.knesset.gov.il/Activity/Committees/Ethics/Pages/default.aspx"
    is_index_page: true
    file_pattern: "*.pdf"
    download_directory: "./downloads/ethics"
    timeout: 60
  versioning_strategy: "combined"
  fetch_strategy: "index_page"
  enabled: true
  priority: 3
  tags: ["legal", "ethics", "decisions", "pdf"]
```

#### 3. Spreadsheet Sources

```yaml
- id: "ethics-rules-spreadsheet"
  name: "Ethics Rules (Spreadsheet)"
  description: "Google Sheets containing ethics rules and decisions"
  type: "spreadsheet"
  spreadsheet_config:
    url: "https://docs.google.com/spreadsheets/d/1fEgiCLNMQQZqBgQFlkABXgke8I2kI1i1XUvj8Yba9Ow/edit?gid=0#gid=0"
    sheet_name: "Ethics Rules"
    range: "A1:D1000"
    use_adc: true
  versioning_strategy: "timestamp"
  fetch_strategy: "async"
  fetch_interval: 3600  # 1 hour
  enabled: true
  priority: 5
  tags: ["legal", "ethics", "rules", "spreadsheet"]
```

## Versioning Strategies

### 1. Hash (`hash`)
- **Description**: Uses SHA-256 hash of content
- **Use Case**: When content changes frequently but you want to detect any change
- **Pros**: Detects any content change, even whitespace
- **Cons**: Requires full content download to compute hash

### 2. Timestamp (`timestamp`)
- **Description**: Uses HTTP Last-Modified header
- **Use Case**: When servers provide reliable timestamps
- **Pros**: Fast, doesn't require content download
- **Cons**: Depends on server providing accurate timestamps

### 3. ETag (`etag`)
- **Description**: Uses HTTP ETag header
- **Use Case**: When servers provide ETags
- **Pros**: Fast, reliable for unchanged content
- **Cons**: Depends on server implementation

### 4. Version String (`version_string`)
- **Description**: Uses explicit version string
- **Use Case**: When you control the versioning
- **Pros**: Human-readable, predictable
- **Cons**: Requires manual version management

### 5. Combined (`combined`)
- **Description**: Uses both hash and timestamp
- **Use Case**: When you want maximum reliability
- **Pros**: Most reliable change detection
- **Cons**: Requires full content download

## Fetch Strategies

### 1. Direct (`direct`)
- **Description**: Direct HTTP GET request
- **Use Case**: Single HTML pages or direct file downloads
- **Pros**: Simple, fast
- **Cons**: Limited to single resources

### 2. Index Page (`index_page`)
- **Description**: Parse index page to find links
- **Use Case**: Directory listings or index pages
- **Pros**: Can discover multiple files
- **Cons**: More complex, depends on page structure

### 3. API (`api`)
- **Description**: Use API endpoints
- **Use Case**: REST APIs or structured data sources
- **Pros**: Structured, reliable
- **Cons**: Requires API documentation

### 4. Async (`async`)
- **Description**: Asynchronous fetching
- **Use Case**: Large sources or non-blocking operations
- **Pros**: Non-blocking, good for large sources
- **Cons**: More complex orchestration

## Configuration Fields

### Global Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `version` | string | "1.0.0" | Configuration schema version |
| `name` | string | required | Configuration name |
| `description` | string | optional | Configuration description |
| `default_versioning_strategy` | enum | "hash" | Default versioning strategy |
| `default_fetch_strategy` | enum | "direct" | Default fetch strategy |
| `cache_directory` | string | "./cache" | Cache directory path |
| `embedding_cache_path` | string | "./cache/embeddings.sqlite" | Embedding cache path |
| `version_cache_path` | string | "./cache/versions.json" | Version cache path |
| `max_concurrent_sources` | int | 5 | Maximum concurrent source processing |
| `timeout_per_source` | int | 300 | Timeout per source in seconds |
| `log_level` | string | "INFO" | Logging level |
| `log_file` | string | optional | Log file path |

### Source Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | yes | Unique source identifier |
| `name` | string | yes | Human-readable source name |
| `description` | string | no | Source description |
| `type` | enum | yes | Source type (html, pdf, spreadsheet) |
| `html_config` | object | conditional | HTML source configuration |
| `pdf_config` | object | conditional | PDF source configuration |
| `spreadsheet_config` | object | conditional | Spreadsheet source configuration |
| `versioning_strategy` | enum | no | Versioning strategy |
| `version_string` | string | no | Explicit version string |
| `fetch_strategy` | enum | no | Fetch strategy |
| `fetch_interval` | int | no | Fetch interval in seconds |
| `enabled` | boolean | no | Whether source is enabled |
| `priority` | int | no | Processing priority (lower = higher) |
| `max_retries` | int | no | Maximum retry attempts |
| `tags` | array | no | Source tags for categorization |
| `metadata` | object | no | Additional metadata |

### HTML Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | required | Source URL |
| `selector` | string | optional | CSS selector for content extraction |
| `encoding` | string | "utf-8" | Content encoding |
| `headers` | object | {} | HTTP headers |
| `timeout` | int | 30 | Request timeout in seconds |
| `retry_attempts` | int | 3 | Number of retry attempts |

### PDF Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | required | Source URL or index page URL |
| `is_index_page` | boolean | false | Whether URL is an index page |
| `file_pattern` | string | optional | File pattern for PDF files |
| `download_directory` | string | optional | Directory to store downloaded PDFs |
| `headers` | object | {} | HTTP headers |
| `timeout` | int | 60 | Request timeout in seconds |

### Spreadsheet Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | required | Google Sheets URL |
| `sheet_name` | string | optional | Specific sheet name |
| `range` | string | optional | Cell range (e.g., 'A1:D100') |
| `credentials_path` | string | optional | Path to service account credentials |
| `use_adc` | boolean | false | Use Application Default Credentials |

## Version Management

The system maintains version information for each source:

```json
{
  "source_id": "knesset-laws-html",
  "version_hash": "abc123...",
  "version_timestamp": "2024-01-15T10:30:00Z",
  "version_string": "v1.0.0",
  "etag": "etag123",
  "content_size": 1024,
  "last_fetch": "2024-01-15T10:30:00Z",
  "fetch_status": "success",
  "error_message": null
}
```

## Usage Examples

### Basic Configuration

```python
from botnim.sync_config import SyncConfig, ContentSource, SourceType

# Load configuration
config = SyncConfig.from_yaml("config.yaml")

# Get sources by type
html_sources = config.get_sources_by_type(SourceType.HTML)

# Get enabled sources
enabled_sources = config.get_enabled_sources()

# Get specific source
source = config.get_source_by_id("knesset-laws-html")
```

### Version Management

```python
from botnim.sync_config import VersionManager

# Initialize version manager
vm = VersionManager("./cache/versions.json")

# Check if content has changed
has_changed = vm.has_changed("source-id", "new-hash")

# Update version information
vm.update_version(version_info)
```

## Validation

The configuration system includes comprehensive validation:

- **URL Format**: Validates URL format for all sources
- **Required Fields**: Ensures required fields are present
- **Source Type Matching**: Validates that source config matches source type
- **Google Sheets URLs**: Validates Google Sheets URL format

## Error Handling

The system provides clear error messages for validation failures:

- `Invalid URL format`: URL doesn't match expected format
- `HTML source requires html_config`: Missing HTML configuration
- `PDF source requires pdf_config`: Missing PDF configuration
- `Spreadsheet source requires spreadsheet_config`: Missing spreadsheet configuration
- `Must be a Google Sheets URL`: Invalid Google Sheets URL

## Best Practices

1. **Use Descriptive IDs**: Use meaningful source IDs that reflect the content
2. **Set Appropriate Priorities**: Use priority to control processing order
3. **Use Tags**: Tag sources for better organization and filtering
4. **Choose Right Versioning**: Select versioning strategy based on source characteristics
5. **Handle Errors**: Implement proper error handling for failed sources
6. **Monitor Performance**: Use logging to monitor sync performance
7. **Test Configurations**: Validate configurations before deployment

## Migration from Existing Configurations

To migrate from existing bot configurations:

1. **Extract Sources**: Identify all content sources from existing configs
2. **Map Types**: Map existing source types to new schema
3. **Configure Versioning**: Choose appropriate versioning strategies
4. **Test Migration**: Validate migrated configurations
5. **Update Orchestration**: Update sync orchestration to use new schema

## Future Extensions

The schema is designed to be extensible:

- **New Source Types**: Easy to add new source types (e.g., RSS feeds, APIs)
- **New Versioning Strategies**: Can add custom versioning logic
- **New Fetch Strategies**: Can implement custom fetching methods
- **Plugin System**: Could support plugin-based extensions 