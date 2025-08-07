# Sync Infrastructure Documentation

This directory contains comprehensive documentation for the automated sync infrastructure system.

## 📚 Documentation Index

### Core Documentation

- **[Sync Configuration Schema](sync_config_schema.md)** - Unified configuration format for all content source types (HTML, PDF, spreadsheets)
- **[Spreadsheet Processing](spreadsheet_processing_documentation.md)** - Asynchronous spreadsheet processing with background task queues
- **[PDF Discovery & Processing](pdf_discovery_documentation.md)** - Automated discovery and processing of PDF files from remote sources
- **[Caching Layer](caching_layer_documentation.md)** - Duplicate detection and content tracking system
- **[Embedding Processing](embedding_processing_documentation.md)** - Cloud-based embedding storage and change detection system
- **[Sync Orchestration](sync_orchestration_documentation.md)** - Comprehensive sync orchestration with CI integration

### Development Documentation

- **[Sync Module Reorganization](sync_module_reorganization.md)** - Documentation of the sync module restructuring

## 🚀 Quick Start

### 1. Configuration

Start by configuring your content sources in a YAML file:

```yaml
# config.yaml
version: "1.0.0"
name: "My Content Sync"
sources:
  - id: "my-spreadsheet"
    name: "My Spreadsheet"
    type: "spreadsheet"
    spreadsheet_config:
      url: "https://docs.google.com/spreadsheets/d/..."
      sheet_name: "Sheet1"
      range: "A1:Z1000"
      use_adc: true
    fetch_strategy: "async"
    enabled: true
```

### 2. Process Content

```bash
# Process spreadsheet sources asynchronously
botnim sync spreadsheet process config.yaml

# Process HTML sources
botnim sync html process config.yaml

# Check processing status
botnim sync spreadsheet status config.yaml
```

### 3. Manage Cache

```bash
# View cache statistics
botnim sync cache stats

# Clean up old entries
botnim sync cache cleanup --older-than 30
```

## 📋 Supported Source Types

### HTML Sources
- Direct HTML content fetching
- CSS selector-based content extraction
- **HTML index page discovery** with automated link discovery and pattern filtering
- **Advanced document parsing and chunking** with AI-powered structure analysis
- Version tracking with content hashing

### PDF Sources
- Automated PDF discovery from index pages
- Temporary download and processing
- Integration with existing PDF pipeline

### Spreadsheet Sources
- **Asynchronous processing** with background task queues
- Google Sheets integration
- Intermediate storage in Elasticsearch
- Task tracking and status monitoring

## 🔧 Key Features

- **Unified Configuration**: Single YAML format for all source types
- **Versioning & Change Detection**: Content hash-based versioning with incremental updates
- **Caching Layer**: SQLite-based duplicate detection and content tracking
- **Asynchronous Processing**: Background processing for spreadsheet sources
- **HTML Index Page Discovery**: Automated discovery and processing of multiple HTML pages from index pages
- **Advanced Document Parsing**: AI-powered document structure analysis and intelligent chunking
- **Cloud-Native Design**: Designed for CI/CD workflows with no local dependencies
- **Comprehensive Logging**: Structured logging with detailed error reporting

## 📖 Detailed Guides

### For Spreadsheet Processing
See [Spreadsheet Processing Documentation](spreadsheet_processing_documentation.md) for:
- Background task queue management
- Google Sheets API integration
- Intermediate storage in Elasticsearch
- CLI commands and programmatic usage
- Error handling and troubleshooting

### For Configuration
See [Sync Configuration Schema](sync_config_schema.md) for:
- Complete configuration format
- Source type specifications
- Versioning strategies
- Validation and error handling
- Best practices and examples

### For PDF Processing
See [PDF Discovery Documentation](pdf_discovery_documentation.md) for:
- Automated PDF discovery
- Temporary download management
- Processing pipeline integration
- Tracking and cleanup

### For Caching
See [Caching Layer Documentation](caching_layer_documentation.md) for:
- Duplicate detection algorithms
- Content tracking and versioning
- Cache management and cleanup
- Performance optimization

## 🛠️ Development

### Running Tests

```bash
# Run all sync tests
python -m pytest botnim/sync/tests/ -v

# Run specific test categories
python -m pytest botnim/sync/tests/test_spreadsheet_fetcher.py -v
python -m pytest botnim/sync/tests/test_config.py -v
python -m pytest botnim/sync/tests/test_cache.py -v
```

### Code Structure

```
botnim/sync/
├── __init__.py              # Module exports
├── config.py                # Configuration schema
├── cache.py                 # Caching layer
├── cli.py                   # CLI commands
├── html_fetcher.py          # HTML content fetching
├── pdf_discovery.py         # PDF discovery and processing
├── spreadsheet_fetcher.py   # Asynchronous spreadsheet processing
└── tests/                   # Test suite
    ├── test_config.py
    ├── test_cache.py
    ├── test_html_fetcher.py
    ├── test_pdf_discovery.py
    └── test_spreadsheet_fetcher.py
```

## 🔗 Related Documentation

- [Main README](../../README.md) - Project overview and getting started
- [PDF Processor Documentation](../document_parser/pdf_processor/) - PDF processing pipeline
- [Vector Store Documentation](../vector_store/) - Elasticsearch integration

## 📞 Support

For issues and questions:
1. Check the relevant documentation file
2. Review error messages and logs
3. Check the test suite for examples
4. Open an issue with detailed error information 