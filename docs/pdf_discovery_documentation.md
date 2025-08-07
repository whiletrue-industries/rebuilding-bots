# PDF Discovery and Processing Documentation

## Overview

The PDF discovery and processing system automatically discovers new PDFs from remote sources, downloads them temporarily, processes them using the existing pipeline, and stores the results in the vector store. This system is designed to work as part of the automated sync infrastructure.

> **Note:** This document describes the direct-to-vector-store PDF processing workflow. For more advanced use cases involving structured data extraction from PDFs into a Google Spreadsheet before vectorization, please see the `pdf_pipeline` source type in the [Sync Configuration Schema](./sync_config_schema.md).

## Key Features

- **Automated Discovery**: Scans remote index pages for new PDF files
- **Temporary Download**: Downloads PDFs to temporary location for processing
- **Duplicate Prevention**: Tracks processed files to avoid reprocessing
- **Integration**: Uses existing PDF processing pipeline
- **Tracking**: Stores processing metadata in Elasticsearch
- **Cleanup**: Automatically removes temporary files after processing

## Architecture

### Core Components

1. **PDFDiscoveryService**: Discovers PDF files from remote index pages
2. **PDFDownloadManager**: Manages temporary download and cleanup
3. **PDFProcessingTracker**: Tracks processing status in Elasticsearch
4. **PDFDiscoveryProcessor**: Main orchestrator for the entire process

### Data Flow

```
Remote Index Page → PDF Discovery → Download → Process → Vector Store
                                      ↓
                              Tracking (Elasticsearch)
```

## Configuration

### Sync Configuration

PDF sources are configured in the sync configuration file (e.g., `specs/takanon/sync_config.yaml`):

```yaml
- id: "ethics-committee-25-pdf"
  name: "החלטות ועדת האתיקה - הכנסת ה-25"
  description: "החלטות PDF מוועדת האתיקה של הכנסת ה-25"
  type: "pdf"
  pdf_config:
    url: "https://main.knesset.gov.il/activity/committees/ethics/pages/committeedecisions25.aspx"
    is_index_page: true
    file_pattern: "*.pdf"
    download_directory: "./downloads/ethics-committee/25"
    timeout: 60
  versioning_strategy: "combined"
  fetch_strategy: "index_page"
  enabled: true
  priority: 6
  tags: ["משפטי", "אתיקה", "החלטות", "pdf", "כנסת-25"]
```

### PDF Processing Configuration

PDF processing configuration is now integrated into the main sync configuration file. Each PDF source includes its own processing configuration:

```yaml
- id: "ethics-committee-25-pdf"
  name: "החלטות ועדת האתיקה - הכנסת ה-25"
  type: "pdf"
  pdf_config:
    url: "https://main.knesset.gov.il/activity/committees/ethics/pages/committeedecisions25.aspx"
    is_index_page: true
    file_pattern: "*.pdf"
    processing:
      model: "gpt-4.1-mini"
      max_tokens: 4000
      temperature: 0.1
      fields:
        - name: "committee_name"
          type: "string"
          description: "Name of the committee that made the decision"
          required: true
        - name: "decision_date"
          type: "date"
          description: "Date when the decision was made"
          required: true
        # ... more fields
      options:
        enable_ocr: true
        ocr_language: "heb+eng"
        chunk_size: 1000
        chunk_overlap: 200
        max_file_size_mb: 50
```

## Usage

### Command Line Interface

#### Discover and Process PDFs

```bash
# Discover and process PDFs from a specific source
python -m botnim.sync.cli pdf-discover \
  --config-file specs/takanon/sync_config.yaml \
  --source-id ethics-committee-25-pdf \
  --environment staging
```

#### Check Processing Status

```bash
# Check processing status for a source
python -m botnim.sync.cli pdf-status \
  --source-id ethics-committee-25-pdf \
  --environment staging \
  --limit 10
```

### Programmatic Usage

```python
from botnim.sync.pdf_discovery import process_pdf_source
from botnim.sync.config import SyncConfig
from botnim.sync.cache import SyncCache
from botnim.vector_store.vector_store_es import VectorStoreES

# Load configuration
config = SyncConfig.from_yaml("specs/takanon/sync_config.yaml")
source = config.get_source_by_id("ethics-committee-25-pdf")

# Initialize components
cache = SyncCache()
vector_store = VectorStoreES(environment="staging")
openai_client = get_openai_client()

# Process PDF source
results = process_pdf_source(
    source=source,
    cache=cache,
    vector_store=vector_store,
    openai_client=openai_client
)

print(f"Discovered: {results['discovered_pdfs']}")
print(f"Processed: {results['processed_pdfs']}")
print(f"Failed: {results['failed_pdfs']}")
```

## Processing Workflow

### 1. Discovery Phase

The system scans the configured index page URL and extracts all PDF links:

- Parses HTML content using BeautifulSoup
- Identifies links ending with `.pdf`
- Applies file pattern filters if specified
- Generates unique identifiers for each PDF

### 2. Duplicate Detection

For each discovered PDF, the system checks if it has already been processed:

- Uses URL hash as unique identifier
- Queries Elasticsearch tracking index
- Skips PDFs with "completed" status

### 3. Download Phase

New PDFs are downloaded to a temporary location:

- Creates temporary directory
- Downloads file with proper headers
- Handles network errors and timeouts
- Tracks download status

### 4. Processing Phase

PDFs are processed using the existing pipeline:

- Extracts text content (with OCR fallback)
- Applies structured field extraction
- Stores results in vector store
- Computes content hash for tracking

### 5. Tracking Phase

Processing status is recorded in Elasticsearch:

- Creates tracking document with metadata
- Records processing timestamps
- Stores error messages if processing fails
- Links to vector store document ID

### 6. Cleanup Phase

Temporary files are removed:

- Deletes downloaded PDF files
- Removes temporary directories
- Logs cleanup operations

## Tracking and Monitoring

### Elasticsearch Tracking Index

The system creates a `pdf_processing_tracker` index in Elasticsearch with the following structure:

```json
{
  "source_id": "ethics-committee-25-pdf",
  "pdf_url": "https://example.com/document.pdf",
  "pdf_filename": "document.pdf",
  "url_hash": "abc123...",
  "download_timestamp": "2024-01-01T00:00:00Z",
  "processing_status": "completed",
  "processing_timestamp": "2024-01-01T00:05:00Z",
  "error_message": null,
  "content_hash": "def456...",
  "vector_store_id": "doc123",
  "metadata": {
    "link_text": "Committee Decision",
    "link_title": "Decision Title"
  }
}
```

### Status Values

- `downloading`: PDF is being downloaded
- `downloaded`: PDF has been downloaded successfully
- `processing`: PDF is being processed
- `completed`: PDF has been processed and indexed
- `failed`: Processing failed with error

### Querying Processing Status

```python
# Query processing status
query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"source_id": "ethics-committee-25-pdf"}},
                {"term": {"processing_status": "completed"}}
            ]
        }
    },
    "sort": [{"processing_timestamp": {"order": "desc"}}]
}

results = vector_store.es.search(
    index="pdf_processing_tracker",
    body=query
)
```

## Error Handling

### Common Error Scenarios

1. **Network Errors**: Download failures due to network issues
2. **Invalid PDFs**: Corrupted or password-protected files
3. **Processing Failures**: Text extraction or field extraction errors
4. **Storage Errors**: Vector store or Elasticsearch issues

### Error Recovery

- Failed downloads are retried up to 3 times
- Processing errors are logged with detailed messages
- Partial failures don't stop the entire process
- Error status is tracked in Elasticsearch

## Integration with Sync Workflow

### As Part of Main Sync

The PDF discovery can be integrated into the main sync workflow:

```python
def run_sync_workflow():
    # Process HTML sources
    process_html_sources()
    
    # Process PDF sources
    process_pdf_sources()
    
    # Process spreadsheet sources
    process_spreadsheet_sources()
```

### Standalone Operation

PDF discovery can also run independently:

```bash
# Run PDF discovery only
python -m botnim.sync.cli pdf-discover \
  --config-file sync_config.yaml \
  --source-id my-pdf-source
```

## Testing

### Unit Tests

Run the test suite:

```bash
python -m pytest botnim/sync/tests/test_pdf_discovery.py -v
```

### Integration Tests

Test with real sources:

```bash
# Test with a small, controlled source
python -m botnim.sync.cli pdf-discover \
  --config-file test_config.yaml \
  --source-id test-pdf-source \
  --environment local
```

## Best Practices

### Configuration

1. **Use Specific File Patterns**: Limit discovery to relevant PDFs
2. **Set Appropriate Timeouts**: Balance between reliability and performance
3. **Enable OCR**: Handle image-based PDFs
4. **Configure Error Handling**: Set retry limits and error thresholds

### Monitoring

1. **Track Processing Status**: Monitor completion rates
2. **Log Errors**: Review failed processing attempts
3. **Monitor Storage**: Track vector store growth
4. **Performance Metrics**: Monitor processing times

### Security

1. **Validate URLs**: Ensure sources are trusted
2. **Limit File Sizes**: Prevent large file downloads
3. **Sanitize Content**: Clean extracted text
4. **Access Control**: Restrict vector store access

## Troubleshooting

### Common Issues

1. **No PDFs Discovered**
   - Check if the index page URL is accessible
   - Verify file pattern matches PDF links
   - Check network connectivity

2. **Download Failures**
   - Verify URL accessibility
   - Check timeout settings
   - Review network configuration

3. **Processing Failures**
   - Check PDF file integrity
   - Verify processing configuration
   - Review error logs

4. **Tracking Issues**
   - Verify Elasticsearch connectivity
   - Check index permissions
   - Review tracking configuration

### Debug Mode

Enable debug logging for detailed troubleshooting:

```python
import logging
logging.getLogger('botnim.sync.pdf_discovery').setLevel(logging.DEBUG)
```

## Future Enhancements

### Planned Features

1. **Incremental Processing**: Process only changed content
2. **Batch Processing**: Process multiple PDFs in parallel
3. **Content Validation**: Verify extracted content quality
4. **Advanced Filtering**: More sophisticated file selection
5. **Webhook Integration**: Notify external systems of new content

### Performance Optimizations

1. **Caching**: Cache discovered PDF lists
2. **Parallel Downloads**: Download multiple PDFs simultaneously
3. **Streaming Processing**: Process PDFs as they download
4. **Compression**: Compress temporary files
